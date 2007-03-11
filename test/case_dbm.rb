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

    def test_store_and_length
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

    def test_store_and_empty
      @db.transaction{|tx|
	assert_equal(true, tx.empty?)
	tx['foo'] = 'apple'
	assert_equal(false, tx.empty?)
      }
      @db.transaction{|tx|
	assert_equal(false, tx.empty?)
      }
    end

    def test_store_and_delete
      @db.transaction{|tx|
	assert_equal(nil, tx.delete('foo'))
	assert_equal(nil, tx.delete('bar'))

	tx['foo'] = 'apple'

	assert_equal('apple', tx.delete('foo'))
	assert_equal(nil,     tx.delete('bar'))

	assert_equal(nil, tx['foo'])
	assert_equal(nil, tx['bar'])
      }

      @db.transaction{|tx|
	assert_equal(nil, tx.delete('foo'))
	assert_equal(nil, tx.delete('bar'))

	assert_equal(nil, tx['foo'])
	assert_equal(nil, tx['bar'])
      }
    end

    def test_delete_commited_data
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
      }

      @db.transaction{|tx|
	assert_equal('apple',  tx['foo'])
	assert_equal('banana', tx['bar'])

	assert_equal('apple', tx.delete('foo'))

	assert_equal(nil,      tx['foo'])
	assert_equal('banana', tx['bar'])
      }

      @db.transaction{|tx|
	assert_equal(nil,      tx['foo'])
	assert_equal('banana', tx['bar'])
      }
    end

    def test_store_and_delete_if
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'

	tx.delete_if{|key, value|
	  key == 'bar' || value == 'orange'
	}

	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
	assert_equal(nil,     tx['baz'])
      }

      @db.transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
	assert_equal(nil,     tx['baz'])
      }
    end

    def test_store_and_delete_if_with_keys
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'

	tx.delete_if('foo', 'bar') {|key, value|
	  key == 'bar' || value == 'orange'
	}

	assert_equal('apple',  tx['foo'])
	assert_equal(nil,      tx['bar'])
	assert_equal('orange', tx['baz'])
      }

      @db.transaction{|tx|
	assert_equal('apple',  tx['foo'])
	assert_equal(nil,      tx['bar'])
	assert_equal('orange', tx['baz'])
      }
    end

    def test_store_and_clear
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'

	tx.clear

	assert_equal(nil, tx['foo'])
	assert_equal(nil, tx['bar'])
	assert_equal(nil, tx['baz'])
      }

      @db.transaction{|tx|
	assert_equal(nil, tx['foo'])
	assert_equal(nil, tx['bar'])
	assert_equal(nil, tx['baz'])
      }
    end

    def test_property
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx.set_property('foo', 'bar', 'banana')

	assert_equal(nil, tx.property('foo', :created_time))
	assert_equal(nil, tx.property('foo', :changed_time))
	assert_equal(nil, tx.property('foo', :modified_time))
	assert_equal(nil, tx.property('foo', :hash))
	assert_equal('banana', tx.property('foo', 'bar'))
	assert_equal(nil, tx.property('foo', 'baz'))

	assert_equal(nil, tx.property('bar', :created_time))
	assert_equal(nil, tx.property('bar', :changed_time))
	assert_equal(nil, tx.property('bar', :modified_time))
	assert_equal(nil, tx.property('bar', :hash))
	assert_equal(nil, tx.property('bar', 'baz'))
      }

      @db.transaction{|tx|
	assert_instance_of(Time, tx.property('foo', :created_time))
	assert_instance_of(Time, tx.property('foo', :changed_time))
	assert_instance_of(Time, tx.property('foo', :modified_time))
	assert_equal(Digest::SHA512.hexdigest('apple'), tx.property('foo', :hash))
	assert_equal('banana', tx.property('foo', 'bar'))

	assert_equal(nil, tx.property('bar', :created_time))
	assert_equal(nil, tx.property('bar', :changed_time))
	assert_equal(nil, tx.property('bar', :modified_time))
	assert_equal(nil, tx.property('bar', :hash))
	assert_equal(nil, tx.property('bar', 'baz'))
      }
    end

    def test_has_property
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx.set_property('foo', 'bar', 'banana')

	assert_equal(false, (tx.property? 'foo', :created_time))
	assert_equal(false, (tx.property? 'foo', :changed_time))
	assert_equal(false, (tx.property? 'foo', :modified_time))
	assert_equal(false, (tx.property? 'foo', :hash))
	assert_equal(true,  (tx.property? 'foo', 'bar'))
	assert_equal(false, (tx.property? 'foo', 'baz'))

	assert_equal(false, (tx.property? 'bar', :created_time))
	assert_equal(false, (tx.property? 'bar', :changed_time))
	assert_equal(false, (tx.property? 'bar', :modified_time))
	assert_equal(false, (tx.property? 'bar', :hash))
	assert_equal(false, (tx.property? 'bar', 'baz'))
      }

      @db.transaction{|tx|
	assert_equal(true, (tx.property? 'foo', :created_time))
	assert_equal(true, (tx.property? 'foo', :changed_time))
	assert_equal(true, (tx.property? 'foo', :modified_time))
	assert_equal(true, (tx.property? 'foo', :hash))
	assert_equal(true, (tx.property? 'foo', 'bar'))
	assert_equal(false, (tx.property? 'foo', 'baz'))

	assert_equal(false, (tx.property? 'bar', :created_time))
	assert_equal(false, (tx.property? 'bar', :changed_time))
	assert_equal(false, (tx.property? 'bar', :modified_time))
	assert_equal(false, (tx.property? 'bar', :hash))
	assert_equal(false, (tx.property? 'bar', 'baz'))
      }
    end

    def test_delete_property
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx.set_property('foo', 'bar', 'banana')
	tx.set_property('foo', 'baz', 'orange')
      }

      @db.transaction{|tx|
	assert_equal('banana', tx.property('foo', 'bar'))
	assert_equal(true,     (tx.property? 'foo', 'bar'))
	assert_equal('orange', tx.property('foo', 'baz'))
	assert_equal(true,     (tx.property? 'foo', 'baz'))

	assert_equal('banana', tx.delete_property('foo', 'bar'))
	assert_equal(nil,      tx.delete_property('foo', 'no_property'))

	assert_equal(nil,      tx.property('foo', 'bar'))
	assert_equal(false,    (tx.property? 'foo', 'bar'))
	assert_equal('orange', tx.property('foo', 'baz'))
	assert_equal(true,     (tx.property? 'foo', 'baz'))
      }

      @db.transaction{|tx|
	assert_equal(nil,      tx.property('foo', 'bar'))
	assert_equal(false,    (tx.property? 'foo', 'bar'))
	assert_equal('orange', tx.property('foo', 'baz'))
	assert_equal(true,     (tx.property? 'foo', 'baz'))
      }
    end

    def test_each_property
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx.set_property('foo', 'bar', 'banana')

	assert_alist = [
	  [ 'bar', proc{|v| assert_equal('banana', v) } ]
	]
	tx.each_property('foo') do |name, value|
	  assert(assert_pair = assert_alist.assoc(name), "name: #{name}")
	  assert_pair[1].call(value)
	  assert_alist.delete(assert_pair)
	end
	assert(assert_alist.empty?)
      }

      @db.transaction{|tx|
	assert_alist = [
	  [ :created_time,  proc{|v| assert_instance_of(Time, v) } ],
	  [ :changed_time,  proc{|v| assert_instance_of(Time, v) } ],
	  [ :modified_time, proc{|v| assert_instance_of(Time, v) } ],
	  [ :hash,          proc{|v| assert_equal(Digest::SHA512.hexdigest('apple'), v) } ],
	  [ 'bar',          proc{|v| assert_equal('banana', v) } ]
	]
	tx.each_property('foo') do |name, value|
	  assert(assert_pair = assert_alist.assoc(name), "name: #{name}")
	  assert_pair[1].call(value)
	  assert_alist.delete(assert_pair)
	end
	assert(assert_alist.empty?)
      }
    end

    def test_commit_and_rollback
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx.commit

	tx.delete('foo')
	tx['bar'] = 'banana'
	tx['baz'] = 'orange'
	tx.rollback

	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
	assert_equal(nil,     tx['baz'])
      }

      @db.transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(nil,     tx['bar'])
	assert_equal(nil,     tx['baz'])
      }
    end

    def test_read_only_transaction
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx.set_property('foo', 'bar', 'banana')
      }

      @db.transaction(true) {|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(true, (tx.key? 'foo'))
	assert_equal([ %w[ foo apple ] ], tx.to_a)
	assert_equal(%w[ foo ], tx.keys)
	assert_equal(%w[ apple ], tx.values)
	assert_equal(1, tx.length)
	assert_equal(false, tx.empty?)

	assert_instance_of(Time, tx.property('foo', :created_time))
	assert_instance_of(Time, tx.property('foo', :changed_time))
	assert_instance_of(Time, tx.property('foo', :modified_time))
	assert_equal(Digest::SHA512.hexdigest('apple'), tx.property('foo', :hash))
	assert_equal('banana', tx.property('foo', 'bar'))

	assert_equal(true, (tx.property? 'foo', :created_time))
	assert_equal(true, (tx.property? 'foo', :changed_time))
	assert_equal(true, (tx.property? 'foo', :modified_time))
	assert_equal(true, (tx.property? 'foo', :hash))
	assert_equal(true, (tx.property? 'foo', 'bar'))

	assert_alist = [
	  [ :created_time,  proc{|v| assert_instance_of(Time, v) } ],
	  [ :changed_time,  proc{|v| assert_instance_of(Time, v) } ],
	  [ :modified_time, proc{|v| assert_instance_of(Time, v) } ],
	  [ :hash,          proc{|v| assert_equal(Digest::SHA512.hexdigest('apple'), v) } ],
	  [ 'bar',          proc{|v| assert_equal('banana', v) } ]
	]
	tx.each_property('foo') do |name, value|
	  assert(assert_pair = assert_alist.assoc(name), "name: #{name}")
	  assert_pair[1].call(value)
	  assert_alist.delete(assert_pair)
	end
	assert(assert_alist.empty?)

	assert_exception(NoMethodError) { tx['foo'] = 'melon' }
	assert_exception(NoMethodError) { tx.delete('foo') }
	assert_exception(NoMethodError) { tx.delete_if{|key, value| value == 'apple' } }
	assert_exception(NoMethodError) { tx.set_property('foo', 'baz', 'orange') }
	assert_exception(NoMethodError) { tx.delete_property('foo', 'bar') }
	assert_exception(NoMethodError) { tx.clear }
	assert_exception(NoMethodError) { tx.commit }
	assert_exception(NoMethodError) { tx.rollback }
      }
    end

    def test_read_only_dbm
      @db.transaction{|tx|
	tx['foo'] = 'apple'
	tx.set_property('foo', 'bar', 'banana')
      }
      @db.shutdown
      @db = nil

      @db = new_dbm(:read_only => true)
      @db.transaction{|tx|
	assert_equal('apple', tx['foo'])
	assert_equal(true, (tx.key? 'foo'))
	assert_equal([ %w[ foo apple ] ], tx.to_a)
	assert_equal(%w[ foo ], tx.keys)
	assert_equal(%w[ apple ], tx.values)
	assert_equal(1, tx.length)
	assert_equal(false, tx.empty?)

	assert_instance_of(Time, tx.property('foo', :created_time))
	assert_instance_of(Time, tx.property('foo', :changed_time))
	assert_instance_of(Time, tx.property('foo', :modified_time))
	assert_equal(Digest::SHA512.hexdigest('apple'), tx.property('foo', :hash))
	assert_equal('banana', tx.property('foo', 'bar'))

	assert_equal(true, (tx.property? 'foo', :created_time))
	assert_equal(true, (tx.property? 'foo', :changed_time))
	assert_equal(true, (tx.property? 'foo', :modified_time))
	assert_equal(true, (tx.property? 'foo', :hash))
	assert_equal(true, (tx.property? 'foo', 'bar'))

	assert_alist = [
	  [ :created_time,  proc{|v| assert_instance_of(Time, v) } ],
	  [ :changed_time,  proc{|v| assert_instance_of(Time, v) } ],
	  [ :modified_time, proc{|v| assert_instance_of(Time, v) } ],
	  [ :hash,          proc{|v| assert_equal(Digest::SHA512.hexdigest('apple'), v) } ],
	  [ 'bar',          proc{|v| assert_equal('banana', v) } ]
	]
	tx.each_property('foo') do |name, value|
	  assert(assert_pair = assert_alist.assoc(name), "name: #{name}")
	  assert_pair[1].call(value)
	  assert_alist.delete(assert_pair)
	end
	assert(assert_alist.empty?)

	assert_exception(NoMethodError) { tx['foo'] = 'melon' }
	assert_exception(NoMethodError) { tx.delete('foo') }
	assert_exception(NoMethodError) { tx.delete_if{|key, value| value == 'apple' } }
	assert_exception(NoMethodError) { tx.set_property('foo', 'baz', 'orange') }
	assert_exception(NoMethodError) { tx.delete_property('foo', 'bar') }
	assert_exception(NoMethodError) { tx.clear }
	assert_exception(NoMethodError) { tx.commit }
	assert_exception(NoMethodError) { tx.rollback }
      }

      assert_exception(DBM::NotWritableError) {
	@db.transaction(false) {|tx|
	  assert_fail('not to reach')
	}
      }
    end
  end
end
