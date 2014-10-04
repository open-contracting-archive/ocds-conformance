require 'base64'
require 'json'
require 'logger'
require 'tempfile'

require 'active_support/cache'
require 'colored'
require 'faraday_middleware'
require 'faraday_middleware/response_middleware'
require 'json-schema'
require 'oj' # because stdlib json loading a 1GB file is bananas
require 'octokit'
require 'zip'

CACHE_DIR = File.expand_path('web_cache', __dir__)
JSON_SCHEMA_URL = 'https://raw.githubusercontent.com/open-contracting/standard/master/standard/schema/release-schema.json'

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

LOGGER = ColorLogger.new('ocds', level: 'INFO')

def initialize_usage(schema, definitions, prefix=nil)
  usage = {}

  schema['properties'].each do |name,definition|
    key = if prefix
      "#{prefix}_#{name}"
    else
      name
    end

    usage[key] = 0

    if definition['$ref'] || definition['items']
      ref = definition['$ref'] || definition['items']['$ref']
      subschema = if ref
        definitions[ref.split('/')[-1]]
      else
        definition['items']
      end
      usage.merge!(initialize_usage(subschema, definitions, key))
    end
  end

  usage
end

def initialize_report(schema, definitions, usage, prefix=nil)
  report = {}

  schema['properties'].each do |name,definition|
    key = if prefix
      "#{prefix}_#{name}"
    else
      name
    end

    if definition['$ref'] || definition['items']
      ref = definition['$ref'] || definition['items']['$ref']
      subschema = if ref
        definitions[ref.split('/')[-1]]
      else
        definition['items']
      end
      report[name] = initialize_report(subschema, definitions, usage, key)
    else
      report[name] = usage.fetch(key)
    end
  end

  report
end

def update_usage(usage, release, prefix=nil)
  release.each do |name,value|
    if !value.nil? && (Fixnum === value || Float === value || value === true || value === false || !value.empty?)
      key = if prefix
        "#{prefix}_#{name}"
      else
        name
      end

      if usage.key?(key)
        usage[key] += 1
      else
        LOGGER.warn("Unrecognized key #{key}: #{release.inspect}")
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

def items_from_github(repo)
  items = {}

  # @see https://github.com/octokit/octokit.rb#caching
  Octokit.middleware = Faraday::RackBuilder.new do |connection|
    connection.response :caching do
      ActiveSupport::Cache::FileStore.new(CACHE_DIR, expires_in: 604800) # 1 week
    end
    connection.use Octokit::Response::RaiseError
    connection.adapter Faraday.default_adapter
  end
  client = Octokit::Client.new

  sha = client.commits(repo, per_page: 1)[0].sha
  response = client.tree(repo, sha, recursive: true)

  if response.truncated?
    LOGGER.warn "#{tree.url} is truncated"
  else
    response.tree.each do |entry|
      if entry.type == 'blob' && File.extname(entry.path) == '.json'
        LOGGER.info "Getting #{entry.path}"

        begin
          blob = client.blob(repo, entry.sha)

          content = blob.content
          if blob.encoding == 'base64'
            content = Base64.decode64(content)
          end
          content = Oj.load(content)

          items[entry.path] = content
        rescue Octokit::BadGateway => e
          LOGGER.error("#{e} #{entry.path}")
        end
      elsif entry.type != 'blob'
        LOGGER.warn "#{entry.path} is not a blob"
      else
        LOGGER.warn "#{entry.path} is not a .json file"
      end
    end
  end

  items
end

def items_from_zip(uri)
  items = {}

  LOGGER.info("Getting #{uri}")

  Tempfile.open('ocds-conformance') do |f|
    f.binmode
    f.write(client.get(uri).body)
    f.rewind

    Zip::File.open(f) do |zipfile|
      zipfile.entries.each do |entry|
        if File.extname(entry.name) == '.json'
          LOGGER.info("Loading #{entry.name}")

          items[entry.name] = Oj.load(zipfile.read(entry))
        else
          LOGGER.warn "#{entry.name} is not a .json file"
        end
      end
    end
  end

  items
end

def process_release(name, release, usage, schema)
  if [Logger::Severity::DEBUG, Logger::Severity::INFO].include?(LOGGER.level)
    print '.'
  end

  update_usage(usage, release)

  # @todo Cut as issues are resolved.
  # @see https://github.com/open-contracting/sample-data/issues/1
  if release['planning']
    release['planning'].fetch('publicHearingNotices', []).each do |item|
      if item['publishedDate'] == ''
        item['publishedDate'] = nil
      end
      if item['isAmendment'] == '1'
        item['isAmendment'] = true
      elsif item['isAmendment'] == '0'
        item['isAmendment'] = false
      elsif item['isAmendment'] == ''
        item['isAmendment'] = nil
      end
      if item['amendment']['amendmentDate'] == ''
        item['amendment']['amendmentDate'] = nil
      end
    end
    release['planning'].fetch('anticipatedMilestones', []).each do |item|
      if item['date'] == ''
        item['date'] = nil
      end
      if item['dateType'] == ''
        item['dateType'] = nil
      end
      item['attachments'].each do |subitem|
        if subitem['lastModified'] == ''
          subitem['lastModified'] = nil
        end
      end
    end
  end
  if release['formation']
    unless ['Open', 'Selective', 'Limited', nil].include?(release['formation']['method'])
      LOGGER.debug("Unrecognized method: #{release['formation']['method'].inspect}")
      release['formation']['method'] = nil
    end
    unless ['Lowest Cost', 'Best Proposal', 'Best Value to Government', 'Single bid only', nil].include?(release['formation']['selectionCriteria'])
      LOGGER.debug("Unrecognized selectionCriteria: #{release['formation']['selectionCriteria'].inspect}")
      release['formation']['selectionCriteria'] = nil
    end
    if release['formation']['tenderPeriod'] && release['formation']['tenderPeriod']['endDate'] && release['formation']['tenderPeriod']['endDate']['Eastern']
      LOGGER.debug("Invalid date: #{release['formation']['tenderPeriod']['endDate']}")
      release['formation']['tenderPeriod']['endDate'] = nil
    end
    release['formation'].fetch('attachments', []).each do |subitem|
      if subitem['lastModified'] == ''
        subitem['lastModified'] = nil
      end
    end
    release['formation'].fetch('itemsToBeProcured', []).each do |item|
      if item['classificationScheme'] == ''
        item['classificationScheme'] = nil
      end
      if item['quantity'] == ''
        item['quantity'] = nil
      end
      if item['valuePerUnit'] && item['valuePerUnit']['amount'] == ''
        item['valuePerUnit']['amount'] = nil
      end
    end
  end
  release['awards'].each do |item|
    if item['notice']['amendment'] && item['notice']['amendment']['amendmentDate'] == 'None'
      item['notice']['amendment']['amendmentDate'] = nil
    end
    item['itemsAwarded'].each do |subitem|
      if subitem['quantity'] == ''
        subitem['quantity'] = nil
      end
      if subitem['valuePerUnit'] && subitem['valuePerUnit']['amount'] == ''
        subitem['valuePerUnit']['amount'] = nil
      end
    end
  end
  release.fetch('contracts', []).each do |item|
    if item['isAmendment'] == '1'
      item['isAmendment'] = true
    elsif item['isAmendment'] == '0'
      item['isAmendment'] = false
    elsif item['isAmendment'] && (item['isAmendment'].to_i > 1 || item['isAmendment'].to_i == 0)
      item['isAmendment'] = nil
    end
    if item['contractValue']['amount'] == ''
      item['contractValue']['amount'] = nil
    elsif item['contractValue']['amount']
      item['contractValue']['amount'] = Float(item['contractValue']['amount'])
    end
    item['itemsContracted'].each do |subitem|
      if subitem['classificationScheme'] == ''
        subitem['classificationScheme'] = nil
      end
      if subitem['quantity'] == ''
        subitem['quantity'] = nil
      end
      if subitem['valuePerUnit']['amount'] == ''
        subitem['valuePerUnit']['amount'] = nil
      end
    end
    item['deliverables'].each do |subitem|
      if subitem['dueDate'] == ''
        subitem['dueDate'] = nil
      end
      if subitem['attachment']['lastModified'] == ''
        subitem['attachment']['lastModified'] = nil
      end
    end
    item['attachments'].each do |subitem|
      if subitem['lastModified'] == ''
        subitem['lastModified'] = nil
      end
    end
  end
  if release['performance']
    release['performance'].fetch('milestones', []).each do |item|
      if item['date'] == ''
        item['date'] = nil
      end
      if item['dateType'] == ''
        item['dateType'] = nil
      end
      item['attachments'].each do |subitem|
        if subitem['lastModified'] == ''
          subitem['lastModified'] = nil
        end
      end
    end
    release['performance'].fetch('reports', []).each do |subitem|
      if subitem['lastModified'] == ''
        subitem['lastModified'] = nil
      end
    end
  end

  begin
    JSON::Validator.validate!(schema, release) # slow
  rescue JSON::Schema::ValidationError => e
    LOGGER.warn "#{name}: #{e.message}\n#{JSON.pretty_generate(release)}"
  end
end

def client
  Faraday.new do |connection|
    connection.response :caching do
      ActiveSupport::Cache::FileStore.new(CACHE_DIR, expires_in: 604800) # 1 week
    end
    connection.response :logger
    connection.adapter Faraday.default_adapter
  end
end

desc 'Validate OCDS datasets'
task :check, [:uri] do |t,args|
  unless args[:uri]
    raise 'Usage: bundle exec rake check[URI]'
  end

  JSON::Validator.cache_schemas = true

  json_schema = Oj.load(client.get(JSON_SCHEMA_URL).body)

  # @todo Cut as issues are resolved.
  # @see https://github.com/open-contracting/standard/issues/67
  json_schema['properties']['releaseDate']['pattern'] = '^[0-9]{4}-[0-9]{2}-[0-9]{2}(?:T[0-9]{2}:[0-9]{2}:[0-9]{2}[-+][0-9]{4})?$'
  json_schema['properties']['releaseDate'].delete('format')
  # @see https://github.com/open-contracting/standard/issues/68
  json_schema['definitions']['identifier']['properties']['uid']['type'] = ['string', 'integer', 'null']
  # @see https://github.com/open-contracting/standard/issues/43
  json_schema['definitions']['address'].delete('required')
  json_schema['definitions'].each do |_,subschema|
    subschema['properties'].each do |name,definition|
      # @see https://github.com/open-contracting/standard/issues/67
      if definition['format'] == 'date-time'
        definition['pattern'] = '^[0-9]{4}-[0-9]{2}-[0-9]{2}(?:T[0-9]{2}:[0-9]{2}:[0-9]{2}[-+][0-9]{4})?$'
        definition.delete('format')
      end
    end
  end
  # @see https://github.com/open-contracting/standard/pull/69
  json_schema['properties']['formation'] = json_schema['properties'].delete('tender')
  json_schema['required'].delete('formationType')

  usage = initialize_usage(json_schema, json_schema.fetch('definitions'))

  repo = args[:uri][%r{://github.com/([^/]+/[^/]+)\z}, 1]

  items = if repo
    items_from_github(repo)
  elsif args[:uri][/\.zip\z/]
    items_from_zip(args[:uri])
  elsif args[:uri][/\.json\z/]
    [Oj.load(client.get(args[:uri]).body)]
  else
    raise "Unrecognized argument #{args[:uri]}"
  end

  items.each do |name,item|
    LOGGER.info "Processing #{name}"

    if item.key?('releases')
      LOGGER.info "#{item['releases'].size} releases"
      item['releases'].each do |release|
        process_release(name, release, usage, json_schema)
      end
    elsif item.key?('records')
      LOGGER.info "#{item['records'].size} records"
      releases = []
      item['records'].each do |record|
        if record.key?('releases')
          record['releases'].each do |release|
            unless release['uri'] # linked release
              process_release(name, release, usage, json_schema)
            end
          end
        end
        if record.key?('compiledRelease')
          process_release(name, record['compiledRelease'], usage, json_schema)
        end
        if record.key?('versionedRelease')
          # @todo Skipping for now, as the schema is not the same as a release.
        end

        report = initialize_report(json_schema, json_schema.fetch('definitions'), usage)

        puts JSON.pretty_generate(abbreviate_report(report))
      end
    end
  end

  report = initialize_report(json_schema, json_schema.fetch('definitions'), usage)

  puts JSON.pretty_generate(abbreviate_report(report))
end
