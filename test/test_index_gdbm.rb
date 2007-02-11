#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/index/gdbm'
require 'higgs/thread'
require 'rubyunit'

module Higgs::IndexTest
  # for ident(1)
  GDBM_CVS_ID = '$Id$'

  class GDBMTest < RUNIT::TestCase
    include Higgs::Index
    include Higgs::Thread

    def setup
      @tmp_dir = 'gdbm_tmp'
      FileUtils.mkdir_p(@tmp_dir)
      @name = File.join(@tmp_dir, 'gdbm_test.db')
    end

    def teardown
      FileUtils.rm_rf(@tmp_dir)
    end

    def gdbm_read_open
      db = GDBM_READ_OPEN.call(@name)
      begin
        r = yield(db)
      ensure
        db.close
      end
      r
    end

    def gdbm_write_open
      db = GDBM_WRITE_OPEN.call(@name)
      begin
        r = yield(db)
      ensure
        db.close
      end
      r
    end

    WORK_COUNT = 100
    THREAD_COUNT = 10

    def test_read_write_single_thread
      gdbm_write_open{|db|
        db['foo'] = '0'
        assert_equal('0', db['foo'])
      }
      gdbm_read_open{|db|
        assert_equal('0', db['foo'])
      }

      WORK_COUNT.times do |i|
        gdbm_write_open{|db|
          assert_equal(i.to_s, db['foo'])
          db['foo'] = db['foo'].succ
          assert_equal(i.to_s.succ, db['foo'])
        }
        gdbm_read_open{|db|
          assert_equal(i.to_s.succ, db['foo'])
        }
      end
    end

    def test_read_write_multithread
      gdbm_write_open{|db|
        barrier = Barrier.new(THREAD_COUNT + 1)

        th_grp = ThreadGroup.new
        THREAD_COUNT.times{|i|
          th_grp.add Thread.new{
            key = "th#{i}"
            barrier.wait
            db[key] = '0'
            db.sync
            WORK_COUNT.times{
              db[key] = db[key].succ
              db.sync
            }
          }
        }

        barrier.wait
        for t in th_grp.list
          t.join
        end

        THREAD_COUNT.times do |i|
          key = "th#{i}"
          assert_equal(WORK_COUNT.to_s, db[key], key)
        end
      }

      gdbm_read_open{|db|
        THREAD_COUNT.times do |i|
          key = "th#{i}"
          assert_equal(WORK_COUNT.to_s, db[key], key)
        end
      }
    end
  end
end
