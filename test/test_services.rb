#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'drb'
require 'fileutils'
require 'higgs/storage'
require 'test/unit'

module Higgs::Test
  class RemoteServicesTest < Test::Unit::TestCase
    include Higgs

    STORAGE_DIR = 'remote_storage'
    REMOTE_SERVICES_URI = 'druby://localhost:31415'

    class << self
      include Higgs

      def start_services
        return if @pid

        storage_name = File.join(STORAGE_DIR, 'foo')
        FileUtils.rm_rf(STORAGE_DIR) unless $DEBUG
        FileUtils.mkdir_p(STORAGE_DIR)

        start_latch = File.join(STORAGE_DIR, '.start')
        stop_latch = File.join(STORAGE_DIR, '.stop')
        @pid = fork{
          begin
            require 'higgs/services'
            require 'logger'

            st = Storage.new(storage_name,
                             :jlog_rotate_max => 0,
                             :logger => proc{|path|
                               logger = Logger.new(path, 1)
                               logger.level = Logger::DEBUG
                               logger
                             })

            sv = RemoteServices.new(:remote_services_uri => REMOTE_SERVICES_URI,
                                    :storage => st)

            FileUtils.touch(start_latch)
            until (File.exist? stop_latch)
              sleep(0.001)      # spin lock
            end
          ensure
            st.shutdown if st
            sv.shutdown if sv
            FileUtils.rm_rf(STORAGE_DIR) unless $DEBUG
          end
        }

        until (File.exist? start_latch)
          # spin lock
        end

        at_exit{
          FileUtils.touch(stop_latch)
          Process.waitpid(@pid)
        }

        DRb.start_service

        nil
      end

      attr_reader :pid
    end

    def setup
      RemoteServicesTest.start_services
      @services = DRbObject.new_with_uri(REMOTE_SERVICES_URI)
      @localhost_check_tmpfile = File.join(STORAGE_DIR, ".localhost_check.#{RemoteServicesTest.pid}")
    end

    def test_alive_service_v1
      alive_service = @services[:alive_service_v1] or flunk
      assert_equal(true, alive_service.call)
    end

    def test_localhost_check_service_v1
      Dir.chdir('/') {
        localhost_check_service = @services[:localhost_check_service_v1] or flunk
        localhost_check_service.call{|localhost_check|
          localhost_check.call
        }
      }
    end

    def test_localhost_check_service_v1_exists_tmpfile
      localhost_check_service = @services[:localhost_check_service_v1] or flunk
      FileUtils.touch(@localhost_check_tmpfile)
      begin
        localhost_check_service.call{|localhost_check|
          localhost_check.call
        }
      ensure
        FileUtils.rm(@localhost_check_tmpfile, :force => true)
      end
    end

    def test_localhost_check_service_v1_RuntimeError_not_found_tmpfile
      localhost_check_service = @services[:localhost_check_service_v1] or flunk
      localhost_check_service.call{|localhost_check|
        FileUtils.rm(@localhost_check_tmpfile, :force => true)
        assert_raise(RuntimeError) { localhost_check.call }
      }
    end

    def test_localhost_check_service_v1_RuntimeError_mismatch_message
      localhost_check_service = @services[:localhost_check_service_v1] or flunk
      localhost_check_service.call{|localhost_check|
        File.open(@localhost_check_tmpfile, 'w') {|f|
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
          f << 'bar'
        }
        assert_raise(RuntimeError) { localhost_check.call }
      }
    end
  end
end
