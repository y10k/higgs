#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/thread'
require 'rubyunit'

module Higgs::IndexTest
  # for ident(1)
  CVS_ID = '$Id$'

  module IndexTest
    include Higgs::Thread

    def setup
      @tmp_dir = 'index_tmp'
      FileUtils.mkdir_p(@tmp_dir)
      @name = File.join(@tmp_dir, 'index_test.db')
    end

    def teardown
      FileUtils.rm_rf(@tmp_dir)
    end

    def db_read_open
      raise NotImplementedError, 'not implemented.'
    end

    def db_write_open
      raise NotImplementedError, 'not implemented.'
    end

    WORK_COUNT = 100
    THREAD_COUNT = 10

    def test_read_write_single_thread
      db_write_open{|db|
        db['foo'] = '0'
        assert_equal('0', db['foo'])
      }
      db_read_open{|db|
        assert_equal('0', db['foo'])
      }

      WORK_COUNT.times do |i|
        db_write_open{|db|
          assert_equal(i.to_s, db['foo'])
          db['foo'] = db['foo'].succ
          assert_equal(i.to_s.succ, db['foo'])
        }
        db_read_open{|db|
          assert_equal(i.to_s.succ, db['foo'])
        }
      end
    end

    def test_read_write_multithread
      db_write_open{|db|
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

      db_read_open{|db|
        THREAD_COUNT.times do |i|
          key = "th#{i}"
          assert_equal(WORK_COUNT.to_s, db[key], key)
        end
      }
    end
  end
end
