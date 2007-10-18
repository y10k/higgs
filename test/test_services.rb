#!/usr/local/bin/ruby

require 'drb'
require 'fileutils'
require 'higgs/storage'
require 'test/unit'

module Higgs::Test
  class RemoteServicesTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @storage_dir = 'remote_storage'
      @storage_name = File.join(@storage_dir, 'foo')
      FileUtils.rm_rf(@storage_dir) # for debug
      FileUtils.mkdir_p(@storage_dir)

      @remote_services_uri = 'druby://localhost:31415'
      @start_latch = File.join(@storage_dir, '.start')
      @stop_latch = File.join(@storage_dir, '.stop')

      @pid = fork{
	require 'higgs/services'
	require 'logger'

	st = Storage.new(@storage_name,
			 :jlog_rotate_max => 0,
			 :logger => proc{|path|
			   logger = Logger.new(path, 1)
			   logger.level = Logger::DEBUG
			   logger
			 })

	sv = RemoteServices.new(:remote_services_uri => @remote_services_uri,
				:storage => st)

	begin
	  FileUtils.touch(@start_latch)
	  until (File.exist? @stop_latch)
	    # spin lock
	  end
	ensure
	  st.shutdown
	  sv.shutdown
	end
      }

      until (File.exist? @start_latch)
	# spin lock
      end

      DRb.start_service
      @services = DRbObject.new_with_uri(@remote_services_uri)
      @localhost_check_tmpfile = File.join(@storage_dir, ".localhost_check.#{@pid}")
    end

    def teardown
      FileUtils.touch(@stop_latch)
      Process.waitpid(@pid)
      FileUtils.rm_rf(@storage_dir) unless $DEBUG
    end

    def test_localhost_check_service_v1
      Dir.chdir('/') {
	localhost_check_service = @services[:localhost_check_service_v1] or flunk
	localhost_check_service.call('foo') {|localhost_check|
	  localhost_check.call
	}
      }
    end

    def test_localhost_check_service_v1_exists_tmpfile
      localhost_check_service = @services[:localhost_check_service_v1] or flunk
      FileUtils.touch(@localhost_check_tmpfile)
      localhost_check_service.call('foo') {|localhost_check|
	localhost_check.call
      }
    end

    def test_localhost_check_service_v1_RuntimeError_not_found_tmpfile
      localhost_check_service = @services[:localhost_check_service_v1] or flunk
      localhost_check_service.call('foo') {|localhost_check|
	FileUtils.rm_rf(@localhost_check_tmpfile)
	assert_raise(RuntimeError) { localhost_check.call }
      }
    end

    def test_localhost_check_service_v1_RuntimeError_mismatch_message
      localhost_check_service = @services[:localhost_check_service_v1] or flunk
      localhost_check_service.call('foo') {|localhost_check|
	File.open(@localhost_check_tmpfile, 'w') {|f|
	  f.binmode
	  f << 'bar'
	}
	assert_raise(RuntimeError) { localhost_check.call }
      }
    end
  end
end
