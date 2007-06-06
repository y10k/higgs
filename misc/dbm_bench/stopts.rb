# storage options

require 'logger'
require 'yaml'

def get_storage_options
  options = {}
  if (File.exist? '.strc') then
    for name, value in YAML.load(IO.read('.strc'))
      options[name.to_sym] = value
    end
  end

  if (options.key? :data_cksum_type) then
    options[:data_cksum_type] = options[:data_cksum_type].to_sym
  end

  if (options.key? :jlog_cksum_type) then
    options[:jlog_cksum_type] = options[:jlog_cksum_type].to_sym
  end

  if (options.key? :log_level) then
    log_level = case (options.delete(:log_level))
                when 'debug'
                  Logger::DEBUG
                when 'info'
                  Logger::INFO
                when 'warn'
                  Logger::WARN
                when 'error'
                  Logger::ERROR
                when 'fatal'
                  Logger::FATAL
                else
                  raise 'unknown log_level'
                end
    options[:logger] = proc{|path|
      logger = Logger.new(path, 1)
      logger.level = log_level
      logger
    }
  end

  options
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
