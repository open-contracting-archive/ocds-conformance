require 'base64'
require 'json'
require 'logger'
require 'tempfile'

require 'active_support/cache'
require 'colored'
require 'faraday_middleware'
require 'faraday_middleware/response_middleware'
require 'json-schema'
require 'nokogiri'
require 'oj' # because stdlib json loading a 1GB file is bananas
require 'octokit'
require 'zip'

require_relative 'lib/color_logger'

LOGGER = ColorLogger.new('ocds', level: 'INFO')

Dir['tasks/*.rake'].each { |r| import r }
