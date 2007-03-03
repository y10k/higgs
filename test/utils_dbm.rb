#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/dbm'
require 'rubyunit'

module Higgs::DBMTest
  # for ident(1)
  CVS_ID = '$Id$'

  module DBMTest
    def dbm_open
      raise NotImplementedError, 'not implemented'
    end

    def new_dbm(options={})
      options[:dbm_open] = dbm_open unless (options.include? :dbm_open)
      Higgs::DBM.new(@name, options)
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

    def test_store_and_fetch
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'
	assert_equal('apple', tx['foo'])
	assert_equal('banana', tx['bar'])
	assert_equal('orange', tx['baz'])
      }
      @db.transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal('banana', tx['bar'])
	assert_equal('orange', tx['baz'])
      }
    end

    def test_fetch_not_defined_value
      @db.transaction{|tx|
	assert_nil(tx['foo'])
	assert_nil(tx['bar'])
	assert_nil(tx['baz'])
      }
    end
  end
end
