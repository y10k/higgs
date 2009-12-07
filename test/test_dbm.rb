#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'fileutils'
require 'higgs/dbm'
require 'logger'
require 'test/unit'

module Higgs::Test
  class DBMTest < Test::Unit::TestCase
    include Higgs

    def setup
      @test_dir = 'dbm_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @st_name = File.join(@test_dir, 'foo')
      @db = DBM.new(@st_name,
                    :logger => proc{|path|
                      logger = Logger.new(path, 1)
                      logger.level = Logger::DEBUG
                      logger
                    })
    end

    def teardown
      @db.shutdown unless @db.shutdown?
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end

    def test_fetch_and_store
      @db.transaction{|tx|
        assert_equal(nil, tx['foo'])
        assert_equal(nil, tx['bar'])

        assert_equal(false, (tx.key? 'foo'))
        assert_equal(false, (tx.key? 'bar'))

        tx['foo'] = 'HALO'
        tx['bar'] = ''

        assert_equal('HALO', tx['foo'])
        assert_equal('', tx['bar'])

        assert_equal(true, (tx.key? 'foo'))
        assert_equal(true, (tx.key? 'bar'))
      }

      @db.transaction{|tx|
        assert_equal('HALO', tx['foo'])
        assert_equal('', tx['bar'])

        assert_equal(true, (tx.key? 'foo'))
        assert_equal(true, (tx.key? 'bar'))
      }
    end

    def test_store_TypeError_cant_convert_into_String
      @db.transaction{|tx|
        tx['foo'] = :foo
        assert_raise(TypeError) { tx.commit }
        tx.rollback
      }
    end

    def test_update_no_use
      @db.transaction{|tx|
        tx['foo'] = ''
      }

      @db.transaction{|tx|
        tx.update('foo') {|s|
          assert_equal(true, s.frozen?)
          case (RUBY_VERSION)
          when /^1\.8\./
            assert_raise(TypeError, 'ruby-1.8') {
              s << 'foo' << ',' << 'bar' << ',' << 'baz'
            }
          when /^1\.9\./
            assert_raise(RuntimeError, 'ruby-1.9') {
              s << 'foo' << ',' << 'bar' << ',' << 'baz'
            }
          else
            flunk("unknown RUBY_VERSION: #{RUBY_VERSION}")
          end
        }

        assert_equal('', tx['foo'], 'not updated')
      }
    end

    def test_rollback
      @db.transaction{|tx|
        tx['foo'] = 'apple'
      }

      @db.transaction{|tx|
        assert_equal('apple', tx['foo'])
        tx['foo'] = 'banana'
        assert_equal('banana', tx['foo'])
        tx.rollback
        assert_equal('apple', tx['foo'])
      }

      @db.transaction{|tx|
        assert_equal('apple', tx['foo'])
      }
    end

    def test_each_key
      @db.transaction{|tx|
        tx['foo'] = 'HALO'
        tx['bar'] = ''

        expected_keys = %w[ foo bar ]
        tx.each_key do |key|
          assert((expected_keys.include? key), key)
          expected_keys.delete(key)
        end
        assert_equal([], expected_keys)
      }

      @db.transaction{|tx|
        expected_keys = %w[ foo bar ]
        tx.each_key do |key|
          assert((expected_keys.include? key), key)
          expected_keys.delete(key)
        end
        assert_equal([], expected_keys)
      }
    end
  end

  class DBMOpenTest < Test::Unit::TestCase
    include Higgs

    def setup
      @test_dir = 'dbm_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @st_name = File.join(@test_dir, 'foo')
    end

    def teardown
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end

    def test_open
      DBM.open(@st_name) {|db|
        db.transaction{|tx|
          tx['foo'] = 'apple'
        }
      }

      DBM.open(@st_name, :read_only => true) {|db|
        db.transaction{|tx|
          assert_equal('apple', tx['foo'])
        }
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
