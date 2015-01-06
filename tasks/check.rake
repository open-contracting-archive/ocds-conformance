desc 'Validate OCDS datasets'
task :check, [:uri] do |t,args|
  CACHE_DIR = File.expand_path('_cache', __dir__)

  def prefix_name(name, prefix)
    if prefix
      "#{prefix}_#{name}"
    else
      name
    end
  end

  def get_subschema(definition, definitions)
    if definition['$ref'] || definition['items']
      ref = definition['$ref'] || definition['items']['$ref']
      if ref
        definitions[ref.split('/')[-1]]
      else
        definition['items']
      end
    end
  end

  # Initialize the usage statistics.
  def initialize_usage(schema, definitions, prefix=nil)
    usage = {}
    schema['properties'].each do |name,definition|
      key = prefix_name(name, prefix)

      usage[key] = 0

      subschema = get_subschema(definition, definitions)
      if subschema
        usage.merge!(initialize_usage(subschema, definitions, key))
      end
    end
    usage
  end

  # Initialize the report, which is a nested version of the usage statistics.
  def initialize_report(schema, definitions, usage, prefix=nil)
    report = {}
    schema['properties'].each do |name,definition|
      key = prefix_name(name, prefix)

      subschema = get_subschema(definition, definitions)
      report[name] = if subschema
        initialize_report(subschema, definitions, usage, key)
      else
        usage.fetch(key)
      end
    end
    report
  end

  # Track the usage of each property of a release.
  def update_usage(usage, release, prefix=nil)
    release.each do |name,value|
      if !value.nil? && (Fixnum === value || Float === value || value === true || value === false || !value.empty?)
        key = prefix_name(name, prefix)

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

  # Fold sections of the report for easier reading.
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

  # @return [Array] a list of items from a GitHub repository
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
    github = Octokit::Client.new

    sha = github.commits(repo, per_page: 1)[0].sha
    response = github.tree(repo, sha, recursive: true)

    if response.truncated?
      LOGGER.warn "#{tree.url} is truncated"
    else
      response.tree.each do |entry|
        if entry.type == 'blob' && File.extname(entry.path) == '.json'
          LOGGER.info "Getting #{entry.path}"

          begin
            blob = github.blob(repo, entry.sha)

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

  # @return [Array] a list of items from a remote ZIP file
  def items_from_zip(uri)
    items = {}

    LOGGER.info("Getting #{uri}")

    Tempfile.open('ocds-conformance') do |f|
      f.binmode
      f.write(http.get(uri).body)
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

  # Update usage statistics and validate release.
  def process_release(name, release, usage, schema)
    if [Logger::Severity::DEBUG, Logger::Severity::INFO].include?(LOGGER.level)
      print '.'
    end

    update_usage(usage, release)

    begin
      JSON::Validator.validate!(schema, release) # slow
    rescue JSON::Schema::ValidationError => e
      LOGGER.warn "#{name}: #{e.message}\n#{JSON.pretty_generate(release)}"
    end
  end

  # @return an HTTP client
  def http
    Faraday.new do |connection|
      connection.response :caching do
        ActiveSupport::Cache::FileStore.new(CACHE_DIR, expires_in: 604800) # 1 week
      end
      connection.response :logger
      connection.adapter Faraday.default_adapter
    end
  end

  # Start of task.
  unless args[:uri]
    raise 'Usage: bundle exec rake check[URI]'
  end

  # Get the items to validate.
  items = if args[:uri][%r{://github.com/([^/]+/[^/]+)\z}, 1]
    items_from_github($1)
  elsif args[:uri][/\.zip\z/]
    items_from_zip(args[:uri])
  elsif args[:uri][/\.json\z/]
    [Oj.load(http.get(args[:uri]).body)]
  else
    raise "Unrecognized argument #{args[:uri]}"
  end

  # Initialize the schema and term usage tracker.
  JSON::Validator.cache_schemas = true
  json_schema = Oj.load(http.get('https://raw.githubusercontent.com/open-contracting/standard/master/standard/schema/release-schema.json').body)
  usage = initialize_usage(json_schema, json_schema.fetch('definitions'))

  # Report after each record and after all releases.
  items.each do |name,item|
    LOGGER.info "Processing #{name}"

    if item.key?('releases')
      LOGGER.info "#{item['releases'].size} releases"
      item['releases'].each do |release|
        process_release(name, release, usage, json_schema)
      end
    elsif item.key?('records')
      LOGGER.info "#{item['records'].size} records"
      item['records'].each do |record|
        if record.key?('releases')
          record['releases'].each do |release|
            unless release['url'] # linked release
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
