#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'fileutils'
require 'higgs'
require 'test/unit'

module Higgs::Test
  class LoadConfTest < Test::Unit::TestCase
    include Higgs

    def setup
      @conf_path = 'load_conf.yml'
      @log_path = 'dummy.log'
    end

    def teardown
      FileUtils.rm_f(@conf_path)
      FileUtils.rm_f(@log_path)
    end

    def test_load_bool
      File.open(@conf_path, 'w:utf-8') {|w|
        w << ":read_only : true\n"
      }
      options = Higgs.load_conf(@conf_path)
      assert_equal(true, options[:read_only])

      File.open(@conf_path, 'w:utf-8') {|w|
        w << ":read_only : false\n"
      }
      options = Higgs.load_conf(@conf_path)
      assert_equal(false, options[:read_only])
    end

    def test_load_number
      File.open(@conf_path, 'w:utf-8') {|w|
        w << ":number_of_io : 4\n"
      }
      options = Higgs.load_conf(@conf_path)
      assert_equal(4, options[:number_of_io])
    end

    def test_load_symbol
      File.open(@conf_path, 'w:utf-8') {|w|
        w << ":data_hash_type : :SHA512\n"
      }
      options = Higgs.load_conf(@conf_path)
      assert_equal(:SHA512, options[:data_hash_type])
    end

    def test_logging_level_debug
      File.open(@conf_path, 'w:utf-8') {|w|
        w << "logging_level: debug\n"
      }
      options = Higgs.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::DEBUG, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_info
      File.open(@conf_path, 'w:utf-8') {|w|
        w << "logging_level: info\n"
      }
      options = Higgs.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::INFO, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_warn
      File.open(@conf_path, 'w:utf-8') {|w|
        w << "logging_level: warn\n"
      }
      options = Higgs.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::WARN, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_error
      File.open(@conf_path, 'w:utf-8') {|w|
        w << "logging_level: error\n"
      }
      options = Higgs.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::ERROR, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_fatal
      File.open(@conf_path, 'w:utf-8') {|w|
        w << "logging_level: fatal\n"
      }
      options = Higgs.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::FATAL, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_unknown
      File.open(@conf_path, 'w:utf-8') {|w|
        w << "logging_level: foo\n"
      }
      assert_raise(RuntimeError) {
        Higgs.load_conf(@conf_path)
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
