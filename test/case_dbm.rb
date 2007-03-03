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

    def test_fetch_not_defined_value
      @db.transaction{|tx|
	assert_nil(tx['foo'])
      }
    end

    def test_store_and_key
      @db.transaction{|tx|
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

    def test_key_not_defined_value
      @db.transaction{|tx|
	assert_equal(false, (tx.key? 'foo'))
      }
    end
  end
end
