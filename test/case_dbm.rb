#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/dbm'
require 'rubyunit'

module Higgs::DBMTest
  # for ident(1)
  CVS_ID = '$Id$'

  module DBMTestCase
    include Higgs

    def dbm_open
      raise NotImplementedError, 'not implemented'
    end

    def new_dbm(options={})
      options[:dbm_open] = dbm_open unless (options.include? :dbm_open)
      DBM.new(@name, options)
    end
    private :new_dbm

    def setup
      @tmp_dir = 'dbm_tmp'
      FileUtils.mkdir_p(@tmp_dir)
      @name = File.join(@tmp_dir, 'dbm_test')
      @db = new_dbm
    end

    def teardown
      @db.shutdown if @db
      FileUtils.rm_rf(@tmp_dir)
    end

    def test_fetch_and_store
      @db.transaction{|tx|
	assert_equal(nil, tx['foo'])
	assert_equal(nil, tx['bar'])

	tx['foo'] = 'apple'

	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
      }

      @db.transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
      }
    end

    def test_store_and_key
      @db.transaction{|tx|
	assert_equal(false, (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))

	tx['foo'] = 'apple'

	assert_equal(true,  (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))
      }

      @db.transaction{|tx|
	assert_equal(true,  (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))
      }
    end

    def test_fetch_and_key
      @db.transaction{|tx|
	tx['foo']		# load to cache
	tx['bar']		# load to cache
	assert_equal(false, (tx.key? 'foo'))
	assert_equal(false, (tx.key? 'bar'))
      }
    end

    def test_store_and_each_key
      @db.transaction{|tx|
	tx.each_key do |key|
	  assert_fail('not to reach')
	end

	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'

	expected_keys = %w[ foo bar baz ]
	tx.each_key do |key|
	  assert_equal(expected_keys.delete(key), key)
	end
	assert(expected_keys.empty?)
      }

      @db.transaction{|tx|
	expected_keys = %w[ foo bar baz ]
	tx.each_key do |key|
	  assert_equal(expected_keys.delete(key), key)
	end
	assert(expected_keys.empty?)
      }
    end

    def test_fetch_and_each_key
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'
      }

      @db.transaction{|tx|
	tx['alice']		# load to cache
	tx['bob']		# load to cache

	expected_keys = %w[ foo bar baz ]
	tx.each_key do |key|
	  assert_equal(expected_keys.delete(key), key)
	end
	assert(expected_keys.empty?)
      }
    end

    def test_store_and_each_value
      @db.transaction{|tx|
	tx.each_value do |value|
	  assert_fail('not to reach')
	end

	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'

	expected_values = %w[ apple banana orange ]
	tx.each_value do |value|
	  assert_equal(expected_values.delete(value), value)
	end
	assert(expected_values.empty?)
      }

      @db.transaction{|tx|
	expected_values = %w[ apple banana orange ]
	tx.each_value do |value|
	  assert_equal(expected_values.delete(value), value)
	end
	assert(expected_values.empty?)
      }
    end

    def test_store_and_each_pair
      @db.transaction{|tx|
	tx.each_pair do |key, value|
	  assert_fail('not to reach')
	end

	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'

	expected_pairs = [ %w[ foo apple ], %w[ bar banana ], %w[ baz orange ] ]
	tx.each_pair do |key, value|
	  assert_equal(expected_pairs.delete([ key, value ]), [ key, value ])
	end
	assert(expected_pairs.empty?)
      }

      @db.transaction{|tx|
	expected_pairs = [ %w[ foo apple ], %w[ bar banana ], %w[ baz orange ] ]
	tx.each_pair do |key, value|
	  assert_equal(expected_pairs.delete([ key, value ]), [ key, value ])
	end
	assert(expected_pairs.empty?)
      }
    end

    def test_store_and_keys
      @db.transaction{|tx|
	assert_equal([], tx.keys)
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'
	assert_equal(%w[ foo bar baz ].sort, tx.keys.sort)
      }
      @db.transaction{|tx|
	assert_equal(%w[ foo bar baz ].sort, tx.keys.sort)
      }
    end

    def test_store_and_values
      @db.transaction{|tx|
	assert_equal([], tx.values)
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'
	assert_equal(%w[ apple banana orange ].sort, tx.values.sort)
      }
      @db.transaction{|tx|
	assert_equal(%w[ apple banana orange ].sort, tx.values.sort)
      }
    end

    def test_length
      @db.transaction{|tx|
	assert_equal(0, tx.length)
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'
	assert_equal(3, tx.length)
      }
      @db.transaction{|tx|
	assert_equal(3, tx.length)
      }
    end

    def test_length
      @db.transaction{|tx|
	assert_equal(true, tx.empty?)
	tx['foo'] = 'apple'
	assert_equal(false, tx.empty?)
      }
      @db.transaction{|tx|
	assert_equal(false, tx.empty?)
      }
    end
  end
end
