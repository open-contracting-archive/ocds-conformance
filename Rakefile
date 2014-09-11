require 'base64'
require 'json'
require 'logger'

require 'active_support/cache'
require 'colored'
require 'faraday_middleware'
require 'faraday_middleware/response_middleware'
require 'json-schema'
require 'octokit'

CACHE_DIR = File.expand_path('web_cache', __dir__)
JSON_SCHEMA_URL = 'https://raw.githubusercontent.com/open-contracting/standard/master/standard/schema/release-schema.json'
LOGGER = ColorLogger.new('ocds')

class ColorLogger
  # Returns a configured logger.
  #
  # @param [String] progname the name of the program performing the logging
  # @param [String] level the log level, one of "DEBUG", "INFO", "WARN",
  #   "ERROR", "FATAL" or "UNKNOWN"
  # @param [String,IO] logdev the log device
  # @return [Logger] a configured logger
  def self.new(progname, level: 'INFO', logdev: STDOUT)
    logger = ::Logger.new(logdev)
    logger.level = ::Logger.const_get(level)
    logger.progname = progname
    logger.formatter = proc do |severity, datetime, progname, msg|
      message = "#{datetime.strftime('%T')} #{severity} #{progname}: #{msg}\n"
      case severity
      when 'DEBUG'
        message.magenta
      when 'INFO'
        message.white
      when 'WARN'
        message.yellow
      when 'ERROR'
        message.red
      when 'FATAL'
        message.bold.red_on_white
      end
    end
    logger
  end
end

def initialize_usage(schema, definitions, prefix=nil)
  usage = {}

  schema['properties'].each do |property_name,property_definition|
    key = if prefix
      "#{prefix}_#{property_name}"
    else
      property_name
    end

    usage[key] = 0

    if property_definition['$ref'] || property_definition['items']
      ref = property_definition['$ref'] || property_definition['items']['$ref']
      subschema = if ref
        definitions[ref.split('/')[-1]]
      else
        property_definition['items']
      end
      usage.merge!(initialize_usage(subschema, definitions, key))
    end
  end

  usage
end

def initialize_report(schema, definitions, usage, prefix=nil)
  report = {}

  schema['properties'].each do |property_name,property_definition|
    key = if prefix
      "#{prefix}_#{property_name}"
    else
      property_name
    end

    if property_definition['$ref'] || property_definition['items']
      ref = property_definition['$ref'] || property_definition['items']['$ref']
      subschema = if ref
        definitions[ref.split('/')[-1]]
      else
        property_definition['items']
      end
      report[property_name] = initialize_report(subschema, definitions, usage, key)
    else
      report[property_name] = usage.fetch(key)
    end
  end

  report
end

def update_usage(usage, release, prefix=nil)
  release.each do |property_name,value|
    if !value.nil? && (Fixnum === value || !value.empty?)
      # @todo Cut as issues are resolved.
      # @see https://github.com/devgateway/ca-app-ocds-export/issues/5
      if property_name == 'countryName'
        property_name = 'country-name'
      end

      key = if prefix
        "#{prefix}_#{property_name}"
      else
        property_name
      end

      if usage.key?(key)
        usage[key] += 1
      else
        LOGGER.warn("Unrecognized key #{key}")
      end

      if Hash === value
        update_usage(usage, value, key)
      elsif Array === value
        value.each do |v|
          update_usage(usage, v, key)
        end
      end
    end
  end
end

def abbreviate_report(report)
  report = report.dup

  report.each do |key,value|
    if Hash === value
      report[key] = abbreviate_report(value)
    end
  end

  if report.values.none?{|value| Hash === value || Array === value}
    if report.values.all?{|value| value.zero?}
      0
    elsif report.values.all?{|value| value.nonzero?}
      1
    else
      report.select{|_,value| value.zero?}.keys
    end
  else
    report.select{|_,value| Array === value || value.zero?}.map do |key,value|
      if Array === value
        {key => value}
      else
        key
      end
    end
  end
end

task :default do
  JSON::Validator.cache_schemas = true

  json_schema = JSON.load(Faraday.get(JSON_SCHEMA_URL).body)
  # @todo Cut as issues are resolved.
  # @see https://github.com/open-contracting/standard/issues/67
  json_schema['properties']['releaseDate']['pattern'] = '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  json_schema['properties']['releaseDate'].delete('format')
  json_schema['definitions']['notice']['properties']['publishedDate']['pattern'] = '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  json_schema['definitions']['notice']['properties']['publishedDate'].delete('format')
  json_schema['definitions']['award']['properties']['awardDate']['pattern'] = '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  json_schema['definitions']['award']['properties']['awardDate'].delete('format')
  # @see https://github.com/open-contracting/standard/issues/68
  json_schema['definitions']['identifier']['properties']['uid']['type'] = ['string', 'integer', 'null']
  # @see https://github.com/open-contracting/standard/issues/43
  json_schema['definitions']['address'].delete('required')
  # @see https://github.com/open-contracting/standard/pull/69
  hash = json_schema['properties']['formation'].delete('oneOf')
  json_schema['properties']['formation']['$ref'] = hash[0].fetch('$ref')

  # @see https://github.com/octokit/octokit.rb#caching
  Octokit.middleware = Faraday::RackBuilder.new do |connection|
    connection.response :caching do
      ActiveSupport::Cache::FileStore.new(CACHE_DIR, expires_in: 604800) # 1 week
    end
    connection.use Octokit::Response::RaiseError
    connection.adapter Faraday.default_adapter
  end
  client = Octokit::Client.new

  usage = initialize_usage(json_schema, json_schema.fetch('definitions'))

  [
    # @see http://www.developmentgateway.org/news/test-driving-open-contracting-data-standard
    'devgateway/ca-app-ocds-export',
  ].each do |repo|
    sha = client.commits(repo, per_page: 1)[0].sha

    response = client.tree(repo, sha, recursive: true)
    if response.truncated?
      raise "#{tree.url} is truncated"
    else
      response.tree.each do |entry|
        if entry.type == 'blob' && File.extname(entry.path) == '.json'
          LOGGER.info entry.path

          blob = client.blob(repo, entry.sha)

          content = blob.content
          if blob.encoding == 'base64'
            content = Base64.decode64(content)
          end
          content = JSON.load(content)

          if repo == 'devgateway/ca-app-ocds-export'
            # @todo Cut as issues are resolved.
            releases = content['releases'].map do |release|
              # @see https://github.com/devgateway/ca-app-ocds-export/issues/3
              case release['formation']['selectionCriteria']
              when 'Lowest price', 'The most economic tender'
                release['formation']['selectionCriteria'] = 'Lowest Cost'
              when 'Not defined', 'Not applicable'
                release['formation'].delete('selectionCriteria')
              end
              # @see https://github.com/devgateway/ca-app-ocds-export/issues/4
              release['awards'].each do |award|
                award['suppliers'].each do |supplier|
                  supplier['id']['uri'].gsub!(' ', '%20')
                end
              end
              release
            end
          else
            releases = [content]
          end

          releases.each do |release|
            print '.'

            update_usage(usage, release)

            begin
              JSON::Validator.validate!(json_schema, release) # slow
            rescue JSON::Schema::ValidationError => e
              puts "#{entry.path}: #{e.message}\n#{JSON.pretty_generate(release)}"
            end
          end
        elsif entry.type != 'blob'
          raise "#{entry.path} is not a blob"
        else
          raise "#{entry.path} is not a .json file"
        end
      end
    end
  end

  report = initialize_report(json_schema, json_schema.fetch('definitions'), usage)

  puts JSON.pretty_generate(abbreviate_report(report))
end
