#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/thread'
require 'rubyunit'

module Higgs::IndexTest
  # for ident(1)
  CVS_ID = '$Id$'

  module IndexTestCase
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

    # assumed DB methods:
    #   * db[key]
    #   * db[key] = value
    #
    def test_read_write
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

    # assumed DB methods:
    #   * db[key]
    #   * db[key] = value
    #
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

    # assumed DB methods:
    #   * db.key?
    #
    def test_key
      db_write_open{|db|
        assert_equal(false, (db.key? 'foo'))
        assert_equal(nil, db['foo'])
      }
      db_read_open{|db|
        assert_equal(false, (db.key? 'foo'))
        assert_equal(nil, db['foo'])
      }
      db_write_open{|db|
        db['foo'] = 'HALO'
        assert_equal(true, (db.key? 'foo'))
        assert_equal('HALO', db['foo'])
      }
      db_read_open{|db|
        assert_equal(true, (db.key? 'foo'))
        assert_equal('HALO', db['foo'])
      }
    end

    # assumed DB methods:
    #   * db.delete(key)
    #
    def test_delete
      db_write_open{|db|
        db['foo'] = 'HALO'
        assert_equal(true, (db.key? 'foo'))
        assert_equal('HALO', db['foo'])
      }
      db_read_open{|db|
        assert_equal(true, (db.key? 'foo'))
        assert_equal('HALO', db['foo'])
      }
      db_write_open{|db|
        db.delete('foo')
        assert_equal(false, (db.key? 'foo'))
        assert_equal(nil, db['foo'])
      }
    end

    # assumed DB methods:
    #   * db.each_key{|key| ... }
    #
    def test_each_keys
      db_write_open{|db|
        db.each_key do |k|
          assert_fail("not exist any key")
        end

        db['foo'] = 'a'
        db['bar'] = 'b'
        db['baz'] = 'c'

        expected_keys = %w[ foo bar baz ]
        db.each_key do |k|
          assert((expected_keys.include? k), "each_key do |#{k}|")
          expected_keys.delete(k)
        end
        assert(expected_keys.empty?)
      }

      db_read_open{|db|
        expected_keys = %w[ foo bar baz ]
        db.each_key do |k|
          assert((expected_keys.include? k), "each_key do |#{k}|")
          expected_keys.delete(k)
        end
        assert(expected_keys.empty?)
      }
    end
  end
end
