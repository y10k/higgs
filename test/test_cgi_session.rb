#!/usr/local/bin/ruby

require 'cgi/session/higgs'
require 'cgi/session/pstore'
require 'fileutils'
require 'logger'
require 'test/unit'

module Higgs::Test
  module CGISessionTest
    # for ident(1)
    CVS_ID = '$Id$'

    def new_store(session, options={})
      options.update('tmpdir' => @tmpdir, :logger => @logger)
      store_type.new(session, options)
    end

    def setup
      @tmpdir = 'cgi_tmp'
      FileUtils.rm_rf(@tmpdir) if $DEBUG
      FileUtils.mkdir_p(@tmpdir)
      @logger = proc{|path|
        logger = Logger.new(path, 1)
        logger.level = Logger::DEBUG
        logger
      }
      @session = Object.new
      class << @session
        attr_accessor :session_id
        attr_accessor :new_session
      end
      @session.session_id = 'foo'
      @session.new_session = true
      @store = new_store(@session)
    end

    def teardown
      @store.close
      FileUtils.rm_rf(@tmpdir) unless $DEBUG
    end

    def test_restore_update_close
      hash = @store.restore
      hash['key'] = { 'k' => 'v' }
      @store.update
      @store.close

      @store = new_store(@session)
      assert_equal({ 'key' => { 'k' => 'v' } }, @store.restore)
    end

    def test_delete
      hash = @store.restore
      hash['key'] = { 'k' => 'v' }
      @store.update
      @store.delete

      @store = new_store(@session)
      assert_equal({}, @store.restore)
    end

    def test_not_new_session
      @store.delete

      @session.new_session = false
      assert_raise(CGI::Session::NoSession) {
        @store = new_store(@session)
      }
    end

    def test_counter
      @store.close
      num_of_procs = 2
      count_by_proc = 100
      pid_list = []
      ready_latch = File.join(@tmpdir, '.ready_latch')
      start_latch = File.join(@tmpdir, '.start_latch')

      num_of_procs.times do |i|
        pid_list << fork{
          FileUtils.touch("#{ready_latch}.#{i}")
          until (File.exist? start_latch)
            # spin lock
          end
          count_by_proc.times do
            @store = new_store(@session)
            hash = @store.restore
            hash['count'] = (hash['count'] || 0).succ
            @store.close
          end
        }
      end

      num_of_procs.times do |i|
        until (File.exist? "#{ready_latch}.#{i}")
          # spin lock
        end
      end
      FileUtils.touch(start_latch)

      for pid in pid_list
        Process.waitpid(pid)
      end

      @store = new_store(@session)
      assert_equal(num_of_procs * count_by_proc, @store.restore['count'])
    end
  end

  class CGISessionPstoreTest < Test::Unit::TestCase
    include CGISessionTest

    # for ident(1)
    CVS_ID = '$Id$'

    def store_type
      CGI::Session::PStore
    end
  end

  class CGISessionHiggsStoreTest < Test::Unit::TestCase
    include CGISessionTest

    # for ident(1)
    CVS_ID = '$Id$'

    def store_type
      CGI::Session::HiggsStore
    end
  end
end
