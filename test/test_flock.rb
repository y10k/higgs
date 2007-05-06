#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/flock'
require 'test/unit'

module Higgs::Test
  class FileLockTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @test_dir = 'flock_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @count_path = File.join(@test_dir, 'count')
      @lock_path = File.join(@test_dir, 'lock')
    end

    def teardown
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end

    def make_flock(*args)
      flock = FileLock.new(@lock_path, *args)
      begin
	yield(flock)
      ensure
	flock.close
      end
    end
    private :make_flock

    def test_write_lock_single_process
      make_flock{|flock|
	File.open(@count_path, 'w') {|f| f << '0' }
	100.times do
	  flock.synchronize(:EX) {
	    value = IO.read(@count_path)
	    value.succ!
	    File.open(@count_path, 'w') {|f| f << value }
	  }
	end
	assert_equal('100', IO.read(@count_path))
      }
    end

    def test_write_lock_multi_process
      File.open(@count_path, 'w') {|f| f << '0' }

      writers = 10
      each_count = 1000
      w_pid_list = []

      writers.times do |i|
	w_pid_list << fork{
	  make_flock{|flock|
	    each_count.times do
	      flock.synchronize(:EX) {
		value = IO.read(@count_path)
		value.succ!
		File.open(@count_path, 'w') {|f| f << value }
	      }
	    end
	  }
	}
      end

      w_st_list = []
      for w_pid in w_pid_list
	Process.waitpid(w_pid)
	w_st_list << $?.exitstatus
      end

      w_st_list.each_with_index do |exitstatus, i|
	assert_equal(0, exitstatus, "writer process: #{i}")
      end

      assert_equal(writers * each_count, IO.read(@count_path).to_i)
    end

    def test_read_write_lock_multi_process
      File.open(@count_path, 'w') {|f| f << '0' }

      writers = 3
      readers = 10
      each_count = 1000
      w_pid_list = []
      r_pid_list = []

      writers.times do |i|
	w_pid_list << fork{
	  make_flock{|flock|
	    each_count.times do
	      flock.synchronize(:EX) {
		File.open(@count_path, 'w') {|f| f << '1' }
		File.open(@count_path, 'w') {|f| f << '0' }
	      }
	    end
	  }
	}
      end

      readers.times do |i|
	r_pid_list << fork{
	  make_flock(true) {|flock|
	    each_count.times do
	      flock.synchronize(:SH) {
		assert_equal('0', IO.read(@count_path))
	      }
	    end
	  }
	}
      end

      w_st_list = []
      for w_pid in w_pid_list
	Process.waitpid(w_pid)
	w_st_list << $?.exitstatus
      end

      r_st_list = []
      for r_pid in r_pid_list
	Process.waitpid(r_pid)
	r_st_list << $?.exitstatus
      end

      w_st_list.each_with_index do |exitstatus, i|
	assert_equal(0, exitstatus, "writer process: #{i}")
      end

      r_st_list.each_with_index do |exitstatus, i|
	assert_equal(0, exitstatus, "reader process: #{i}")
      end
    end

    def test_read_only_lock_failed_to_write_lock
      make_flock(true) {|flock|
	assert_raise(RuntimeError) {
	  flock.write_lock
	}
      }
    end

    def test_synchronize_unknown_mode_error
      make_flock(true) {|flock|
	assert_raise(ArgumentError) {
	  flock.synchronize(:UNKNOWN) {
	    flunk('not to reach')
	  }
	}
      }
    end
  end
end
