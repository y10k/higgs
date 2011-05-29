#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'fileutils'
require 'higgs/storage'
require 'test/unit'

module Higgs::Test
  class StorageConfTest < Test::Unit::TestCase
    include Higgs

    def setup
      @conf_path = 'storage_conf.yml'
      @log_path = 'dummy.log'
    end

    def teardown
      FileUtils.rm_f(@conf_path)
      FileUtils.rm_f(@log_path)
    end

    def test_read_only
      File.open(@conf_path, 'w') {|w|
        w << "read_only: true\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_equal(true, options[:read_only])

      File.open(@conf_path, 'w') {|w|
        w << "read_only: false\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_equal(false, options[:read_only])
    end

    def test_number_of_io
      File.open(@conf_path, 'w') {|w|
        w << "number_of_io: 4\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_equal(4, options[:number_of_io])
    end

    def test_data_hash_type
      File.open(@conf_path, 'w') {|w|
        w << "data_hash_type: SHA512\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_equal(:SHA512, options[:data_hash_type])
    end

    def test_jlog_sync
      File.open(@conf_path, 'w') {|w|
        w << "jlog_sync: true\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_equal(true, options[:jlog_sync])

      File.open(@conf_path, 'w') {|w|
        w << "jlog_sync: false\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_equal(false, options[:jlog_sync])
    end

    def test_jlog_hash_type
      File.open(@conf_path, 'w') {|w|
        w << "jlog_hash_type: SHA512\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_equal(:SHA512, options[:jlog_hash_type])
    end

    def test_jlog_rotate_size
      File.open(@conf_path, 'w') {|w|
        w << "jlog_rotate_size: 33554432\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_equal(33554432, options[:jlog_rotate_size])
    end

    def test_jlog_rotate_max
      File.open(@conf_path, 'w') {|w|
        w << "jlog_rotate_max: 0\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_equal(0, options[:jlog_rotate_max])
    end

    def test_properties_cache_limit_size
      File.open(@conf_path, 'w') {|w|
        w << "properties_cache_limit_size: 256\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_instance_of(LRUCache, options[:properties_cache]) # autoload `higgs/cache'
      assert_equal(256, options[:properties_cache].limit_size)
    end

    def test_master_cache_limit_size
      File.open(@conf_path, 'w') {|w|
        w << "master_cache_limit_size: 256\n"
      }
      options = Storage.load_conf(@conf_path)
      assert_instance_of(LRUCache, options[:master_cache]) # autoload `higgs/cache'
      assert_equal(256, options[:master_cache].limit_size)
    end

    def test_logging_level_debug
      File.open(@conf_path, 'w') {|w|
        w << "logging_level: debug\n"
      }
      options = Storage.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::DEBUG, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_info
      File.open(@conf_path, 'w') {|w|
        w << "logging_level: info\n"
      }
      options = Storage.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::INFO, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_warn
      File.open(@conf_path, 'w') {|w|
        w << "logging_level: warn\n"
      }
      options = Storage.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::WARN, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_error
      File.open(@conf_path, 'w') {|w|
        w << "logging_level: error\n"
      }
      options = Storage.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::ERROR, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_fatal
      File.open(@conf_path, 'w') {|w|
        w << "logging_level: fatal\n"
      }
      options = Storage.load_conf(@conf_path)
      logger = options[:logger].call(@log_path)
      begin
        assert_instance_of(Logger, logger) # autoload `logger'
        assert_equal(Logger::FATAL, logger.level)
      ensure
        logger.close
      end
    end

    def test_logging_level_unknown
      File.open(@conf_path, 'w') {|w|
        w << "logging_level: foo\n"
      }
      assert_raise(RuntimeError) {
        Storage.load_conf(@conf_path)
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
