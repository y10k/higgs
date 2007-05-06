#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/storage'
require 'higgs/tman'
require 'test/unit'

module Higgs::Test
  class TransactionManagerTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @test_dir = 'st_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @name = File.join(@test_dir, 'foo')
      @st = Storage.new(@name)
      @tman = TransactionManager.new(@st)
    end

    def teardown
      @st.shutdown unless @st.shutdown?
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end

    def test_transaction
      count = 0
      @tman.transaction(false) {|tx|
        assert_instance_of(Higgs::ReadWriteTransactionContext, tx)
        count += 1
      }
      assert_equal(1, count)

      count = 0
      @tman.transaction(true) {|tx|
        assert_instance_of(Higgs::ReadOnlyTransactionContext, tx)
        count += 1
      }
      assert_equal(1, count)
    end

    def test_fetch_and_store
      @tman.transaction{|tx|
        assert_equal(nil, tx[:foo])
        assert_equal(nil, tx[:bar])

        tx[:foo] = 'apple'

        assert_equal('apple', tx[:foo])
        assert_equal(nil,     tx[:bar])
      }

      assert_equal('apple', @st.fetch(:foo))

      @tman.transaction{|tx|
        assert_equal('apple', tx[:foo])
        assert_equal(nil,     tx[:bar])
      }
    end

    def test_store_and_key
      @tman.transaction{|tx|
        assert_equal(false, (tx.key? :foo))
        assert_equal(false, (tx.key? :bar))

        tx[:foo] = 'apple'

        assert_equal(true,  (tx.key? :foo))
        assert_equal(false, (tx.key? :bar))
      }

      assert_equal(true, (@st.key? :foo))

      @tman.transaction{|tx|
        assert_equal(true,  (tx.key? :foo))
        assert_equal(false, (tx.key? :bar))
      }
    end

    def test_fetch_and_key
      @tman.transaction{|tx|
        tx[:foo]                # load to cache
        tx[:bar]                # load to cache
        assert_equal(false, (tx.key? :foo))
        assert_equal(false, (tx.key? :bar))
      }

      assert_equal(false, (@st.key? :foo))
      assert_equal(false, (@st.key? :bar))
    end

    def test_store_and_each_key
      @tman.transaction{|tx|
        tx.each_key do |key|
          flunk('not to reach')
        end

        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'

        expected_keys = [ :foo, :bar, :baz ]
        tx.each_key do |key|
          assert_equal(expected_keys.delete(key), key)
        end
        assert(expected_keys.empty?)
      }

      expected_keys = [ :foo, :bar, :baz ]
      @st.each_key do |key|
        assert_equal(expected_keys.delete(key), key)
      end
      assert(expected_keys.empty?)

      @tman.transaction{|tx|
        expected_keys = [ :foo, :bar, :baz ]
        tx.each_key do |key|
          assert_equal(expected_keys.delete(key), key)
        end
        assert(expected_keys.empty?)
      }
    end

    def test_fetch_and_each_key
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'
      }

      expected_keys = [ :foo, :bar, :baz ]
      @st.each_key do |key|
        assert_equal(expected_keys.delete(key), key)
      end
      assert(expected_keys.empty?)

      @tman.transaction{|tx|
        tx['alice']             # load to cache
        tx['bob']               # load to cache

        expected_keys = [ :foo, :bar, :baz ]
        tx.each_key do |key|
          assert_equal(expected_keys.delete(key), key)
        end
        assert(expected_keys.empty?)
      }
    end

    def test_store_and_each_value
      @tman.transaction{|tx|
        tx.each_value do |value|
          flunk('not to reach')
        end

        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'

        expected_values = %w[ apple banana orange ]
        tx.each_value do |value|
          assert_equal(expected_values.delete(value), value)
        end
        assert(expected_values.empty?)
      }

      @tman.transaction{|tx|
        expected_values = %w[ apple banana orange ]
        tx.each_value do |value|
          assert_equal(expected_values.delete(value), value)
        end
        assert(expected_values.empty?)
      }
    end

    def test_store_and_each_pair
      @tman.transaction{|tx|
        tx.each_pair do |key, value|
          flunk('not to reach')
        end

        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'

        expected_pairs = [ [ :foo, 'apple' ], [ :bar, 'banana' ], [ :baz, 'orange' ] ]
        tx.each_pair do |key, value|
          assert_equal(expected_pairs.delete([ key, value ]), [ key, value ])
        end
        assert(expected_pairs.empty?)
      }

      @tman.transaction{|tx|
        expected_pairs = [ [ :foo, 'apple' ], [ :bar, 'banana' ], [ :baz, 'orange' ] ]
        tx.each_pair do |key, value|
          assert_equal(expected_pairs.delete([ key, value ]), [ key, value ])
        end
        assert(expected_pairs.empty?)
      }
    end

    def test_store_and_keys
      @tman.transaction{|tx|
        assert_equal([], tx.keys)
        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'
        assert_equal([ :foo, :bar, :baz ].sort{|a, b| a.to_s <=> b.to_s },
                     tx.keys.sort{|a, b| a.to_s <=> b.to_s })
      }

      @tman.transaction{|tx|
        assert_equal([ :foo, :bar, :baz ].sort{|a, b| a.to_s <=> b.to_s },
                     tx.keys.sort{|a, b| a.to_s <=> b.to_s })
      }
    end

    def test_store_and_values
      @tman.transaction{|tx|
        assert_equal([], tx.values)
        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'
        assert_equal(%w[ apple banana orange ].sort, tx.values.sort)
      }

      @tman.transaction{|tx|
        assert_equal(%w[ apple banana orange ].sort, tx.values.sort)
      }
    end

    def test_store_and_length
      @tman.transaction{|tx|
        assert_equal(0, tx.length)
        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'
        assert_equal(3, tx.length)
      }

      @tman.transaction{|tx|
        assert_equal(3, tx.length)
      }
    end

    def test_store_and_empty
      @tman.transaction{|tx|
        assert_equal(true, tx.empty?)
        tx[:foo] = 'apple'
        assert_equal(false, tx.empty?)
      }

      @tman.transaction{|tx|
        assert_equal(false, tx.empty?)
      }
    end

    def test_store_and_delete
      @tman.transaction{|tx|
        assert_equal(nil, tx.delete(:foo))
        assert_equal(nil, tx.delete(:bar))

        tx[:foo] = 'apple'

        assert_equal('apple', tx.delete(:foo))
        assert_equal(nil,     tx.delete(:bar))

        assert_equal(nil, tx[:foo])
        assert_equal(nil, tx[:bar])
      }

      assert_equal(nil, @st.fetch(:foo))
      assert_equal(nil, @st.fetch(:bar))

      @tman.transaction{|tx|
        assert_equal(nil, tx.delete(:foo))
        assert_equal(nil, tx.delete(:bar))

        assert_equal(nil, tx[:foo])
        assert_equal(nil, tx[:bar])
      }
    end

    def test_delete_commited_data
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
      }

      assert_equal('apple',  @st.fetch(:foo))
      assert_equal('banana', @st.fetch(:bar))

      @tman.transaction{|tx|
        assert_equal('apple',  tx[:foo])
        assert_equal('banana', tx[:bar])

        assert_equal('apple', tx.delete(:foo))

        assert_equal(nil,      tx[:foo])
        assert_equal('banana', tx[:bar])
      }

      assert_equal(nil,      @st.fetch(:foo))
      assert_equal('banana', @st.fetch(:bar))

      @tman.transaction{|tx|
        assert_equal(nil,      tx[:foo])
        assert_equal('banana', tx[:bar])
      }
    end

    def test_store_and_delete_if
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'

        tx.delete_if{|key, value|
          key == :bar || value == 'orange'
        }

        assert_equal('apple', tx[:foo])
        assert_equal(nil,     tx[:bar])
        assert_equal(nil,     tx[:baz])
      }

      assert_equal('apple', @st.fetch(:foo))
      assert_equal(nil,     @st.fetch(:bar))
      assert_equal(nil,     @st.fetch(:baz))

      @tman.transaction{|tx|
        assert_equal('apple', tx[:foo])
        assert_equal(nil,     tx[:bar])
        assert_equal(nil,     tx[:baz])
      }
    end

    def test_store_and_delete_if_with_keys
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'

        tx.delete_if(:foo, :bar) {|key, value|
          key == :bar || value == 'orange'
        }

        assert_equal('apple',  tx[:foo])
        assert_equal(nil,      tx[:bar])
        assert_equal('orange', tx[:baz])
      }

      assert_equal('apple',  @st.fetch(:foo))
      assert_equal(nil,      @st.fetch(:bar))
      assert_equal('orange', @st.fetch(:baz))

      @tman.transaction{|tx|
        assert_equal('apple',  tx[:foo])
        assert_equal(nil,      tx[:bar])
        assert_equal('orange', tx[:baz])
      }
    end

    def test_store_and_clear
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'

        tx.clear

        assert_equal(nil, tx[:foo])
        assert_equal(nil, tx[:bar])
        assert_equal(nil, tx[:baz])
      }

      assert_equal(nil, @st.fetch(:foo))
      assert_equal(nil, @st.fetch(:bar))
      assert_equal(nil, @st.fetch(:baz))

      @tman.transaction{|tx|
        assert_equal(nil, tx[:foo])
        assert_equal(nil, tx[:bar])
        assert_equal(nil, tx[:baz])
      }
    end

    def test_property
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx.set_property(:foo, 'bar', 'banana')

        assert_equal(nil, tx.property(:foo, :created_time))
        assert_equal(nil, tx.property(:foo, :changed_time))
        assert_equal(nil, tx.property(:foo, :modified_time))
        assert_equal(nil, tx.property(:foo, :hash_type))
        assert_equal(nil, tx.property(:foo, :hash_value))
        assert_equal('banana', tx.property(:foo, 'bar'))
        assert_equal(nil, tx.property(:foo, 'baz'))

        assert_equal(nil, tx.property(:bar, :created_time))
        assert_equal(nil, tx.property(:bar, :changed_time))
        assert_equal(nil, tx.property(:bar, :modified_time))
        assert_equal(nil, tx.property(:bar, :hash_type))
        assert_equal(nil, tx.property(:bar, :hash_value))
        assert_equal(nil, tx.property(:bar, 'baz'))
      }

      assert_instance_of(Time, @st.fetch_properties(:foo)['system_properties']['created_time'])
      assert_instance_of(Time, @st.fetch_properties(:foo)['system_properties']['changed_time'])
      assert_instance_of(Time, @st.fetch_properties(:foo)['system_properties']['modified_time'])
      assert_equal('SHA512', @st.fetch_properties(:foo)['system_properties']['hash_type'])
      assert_equal(Digest::SHA512.hexdigest('apple'), @st.fetch_properties(:foo)['system_properties']['hash_value'])
      assert_equal('banana', @st.fetch_properties(:foo)['custom_properties']['bar'])
      assert_nil(@st.fetch_properties(:bar))

      @tman.transaction{|tx|
        assert_instance_of(Time, tx.property(:foo, :created_time))
        assert_instance_of(Time, tx.property(:foo, :changed_time))
        assert_instance_of(Time, tx.property(:foo, :modified_time))
        assert_equal('SHA512', tx.property(:foo, :hash_type))
        assert_equal(Digest::SHA512.hexdigest('apple'), tx.property(:foo, :hash_value))
        assert_equal('banana', tx.property(:foo, 'bar'))

        assert_equal(nil, tx.property(:bar, :created_time))
        assert_equal(nil, tx.property(:bar, :changed_time))
        assert_equal(nil, tx.property(:bar, :modified_time))
        assert_equal(nil, tx.property(:bar, :hash_type))
        assert_equal(nil, tx.property(:bar, :hash_value))
        assert_equal(nil, tx.property(:bar, 'baz'))
      }
    end

    def test_has_property
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx.set_property(:foo, 'bar', 'banana')

        assert_equal(false, (tx.property? :foo, :created_time))
        assert_equal(false, (tx.property? :foo, :changed_time))
        assert_equal(false, (tx.property? :foo, :modified_time))
        assert_equal(false, (tx.property? :foo, :hash_type))
        assert_equal(false, (tx.property? :foo, :hash_value))
        assert_equal(true,  (tx.property? :foo, 'bar'))
        assert_equal(false, (tx.property? :foo, 'baz'))

        assert_equal(false, (tx.property? :bar, :created_time))
        assert_equal(false, (tx.property? :bar, :changed_time))
        assert_equal(false, (tx.property? :bar, :modified_time))
        assert_equal(false, (tx.property? :bar, :hash_type))
        assert_equal(false, (tx.property? :bar, :hash_value))
        assert_equal(false, (tx.property? :bar, 'baz'))
      }

      @tman.transaction{|tx|
        assert_equal(true, (tx.property? :foo, :created_time))
        assert_equal(true, (tx.property? :foo, :changed_time))
        assert_equal(true, (tx.property? :foo, :modified_time))
        assert_equal(true, (tx.property? :foo, :hash_type))
        assert_equal(true, (tx.property? :foo, :hash_value))
        assert_equal(true, (tx.property? :foo, 'bar'))
        assert_equal(false, (tx.property? :foo, 'baz'))

        assert_equal(false, (tx.property? :bar, :created_time))
        assert_equal(false, (tx.property? :bar, :changed_time))
        assert_equal(false, (tx.property? :bar, :modified_time))
        assert_equal(false, (tx.property? :bar, :hash_type))
        assert_equal(false, (tx.property? :bar, :hash_value))
        assert_equal(false, (tx.property? :bar, 'baz'))
      }
    end

    def test_delete_property
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx.set_property(:foo, 'bar', 'banana')
        tx.set_property(:foo, 'baz', 'orange')
      }

      assert_equal('banana', @st.fetch_properties(:foo)['custom_properties']['bar'])
      assert_equal('orange', @st.fetch_properties(:foo)['custom_properties']['baz'])

      @tman.transaction{|tx|
        assert_equal('banana', tx.property(:foo, 'bar'))
        assert_equal(true,     (tx.property? :foo, 'bar'))
        assert_equal('orange', tx.property(:foo, 'baz'))
        assert_equal(true,     (tx.property? :foo, 'baz'))

        assert_equal('banana', tx.delete_property(:foo, 'bar'))
        assert_equal(nil,      tx.delete_property(:foo, 'no_property'))

        assert_equal(nil,      tx.property(:foo, 'bar'))
        assert_equal(false,    (tx.property? :foo, 'bar'))
        assert_equal('orange', tx.property(:foo, 'baz'))
        assert_equal(true,     (tx.property? :foo, 'baz'))
      }

      assert_equal(false,   (@st.fetch_properties(:foo)['custom_properties'].key? 'bar'))
      assert_equal('orange', @st.fetch_properties(:foo)['custom_properties']['baz'])

      @tman.transaction{|tx|
        assert_equal(nil,      tx.property(:foo, 'bar'))
        assert_equal(false,    (tx.property? :foo, 'bar'))
        assert_equal('orange', tx.property(:foo, 'baz'))
        assert_equal(true,     (tx.property? :foo, 'baz'))
      }
    end

    def test_each_property
      @tman.transaction{|tx|
        tx[:foo] = 'apple'

        tx.each_property(:foo) do |name, value|
          flunk('not to reach')
        end

        tx.set_property(:foo, 'bar', 'banana')

        assert_alist = [
          [ 'bar', proc{|n, v| assert_equal('banana', v, "name: #{n}") } ]
        ]
        tx.each_property(:foo) do |name, value|
          assert(assert_pair = assert_alist.assoc(name), "name: #{name}")
          assert_pair[1].call(name, value)
          assert_alist.delete(assert_pair)
        end
        assert(assert_alist.empty?)
      }

      @tman.transaction{|tx|
        assert_alist = [
          [ :created_time,  proc{|n, v| assert_instance_of(Time, v, "name: #{n}") } ],
          [ :changed_time,  proc{|n, v| assert_instance_of(Time, v, "name: #{n}") } ],
          [ :modified_time, proc{|n, v| assert_instance_of(Time, v, "name: #{n}") } ],
          [ :hash_type,     proc{|n, v| assert_equal('SHA512', v, "name: #{n}") } ],
          [ :hash_value,    proc{|n, v| assert_equal(Digest::SHA512.hexdigest('apple'), v, "name: #{n}") } ],
          [ 'bar',          proc{|n, v| assert_equal('banana', v, "name: #{n}") } ]
        ]
        tx.each_property(:foo) do |name, value|
          assert(assert_pair = assert_alist.assoc(name), "name: #{name}")
          assert_pair[1].call(name, value)
          assert_alist.delete(assert_pair)
        end
        assert(assert_alist.empty?)
      }
    end

    def test_commit_and_rollback
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx.commit

        tx.delete(:foo)
        tx[:bar] = 'banana'
        tx[:baz] = 'orange'
        tx.rollback

        assert_equal('apple', tx[:foo])
        assert_equal(nil,     tx[:bar])
        assert_equal(nil,     tx[:baz])
      }

      assert_equal('apple', @st.fetch(:foo))
      assert_equal(nil,     @st.fetch(:bar))
      assert_equal(nil,     @st.fetch(:baz))

      @tman.transaction{|tx|
        assert_equal('apple', tx[:foo])
        assert_equal(nil,     tx[:bar])
        assert_equal(nil,     tx[:baz])
      }
    end

    def test_read_only_transaction
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx.set_property(:foo, 'bar', 'banana')
      }

      @tman.transaction(true) {|tx|
        assert_equal('apple', tx[:foo])
        assert_equal(true, (tx.key? :foo))
        assert_equal([ [ :foo, 'apple' ] ], tx.to_a)
        assert_equal([ :foo ], tx.keys)
        assert_equal([ 'apple' ], tx.values)
        assert_equal(1, tx.length)
        assert_equal(false, tx.empty?)

        assert_instance_of(Time, tx.property(:foo, :created_time))
        assert_instance_of(Time, tx.property(:foo, :changed_time))
        assert_instance_of(Time, tx.property(:foo, :modified_time))
        assert_equal('SHA512', tx.property(:foo, :hash_type))
        assert_equal(Digest::SHA512.hexdigest('apple'), tx.property(:foo, :hash_value))
        assert_equal('banana', tx.property(:foo, 'bar'))

        assert_equal(true, (tx.property? :foo, :created_time))
        assert_equal(true, (tx.property? :foo, :changed_time))
        assert_equal(true, (tx.property? :foo, :modified_time))
        assert_equal(true, (tx.property? :foo, :hash_type))
        assert_equal(true, (tx.property? :foo, :hash_value))
        assert_equal(true, (tx.property? :foo, 'bar'))

        assert_alist = [
          [ :created_time,  proc{|n, v| assert_instance_of(Time, v, "name: #{n}") } ],
          [ :changed_time,  proc{|n, v| assert_instance_of(Time, v, "name: #{n}") } ],
          [ :modified_time, proc{|n, v| assert_instance_of(Time, v, "name: #{n}") } ],
          [ :hash_type,     proc{|n, v| assert_equal('SHA512', v, "name: #{n}") } ],
          [ :hash_value,    proc{|n, v| assert_equal(Digest::SHA512.hexdigest('apple'), v, "name: #{n}") } ],
          [ 'bar',          proc{|n, v| assert_equal('banana', v, "name: #{n}") } ]
        ]
        tx.each_property(:foo) do |name, value|
          assert(assert_pair = assert_alist.assoc(name), "name: #{name}")
          assert_pair[1].call(name, value)
          assert_alist.delete(assert_pair)
        end
        assert(assert_alist.empty?)

        assert_raise(NoMethodError) { tx[:foo] = 'melon' }
        assert_raise(NoMethodError) { tx.delete(:foo) }
        assert_raise(NoMethodError) { tx.delete_if{|key, value| value == 'apple' } }
        assert_raise(NoMethodError) { tx.set_property(:foo, 'baz', 'orange') }
        assert_raise(NoMethodError) { tx.delete_property(:foo, 'bar') }
        assert_raise(NoMethodError) { tx.clear }
        assert_raise(NoMethodError) { tx.commit }
        assert_raise(NoMethodError) { tx.rollback }
      }
    end

    def test_read_only_manager
      @tman.transaction{|tx|
        tx[:foo] = 'apple'
        tx.set_property(:foo, 'bar', 'banana')
      }
      @tman = nil

      @tman = TransactionManager.new(@st, :read_only => true)
      @tman.transaction{|tx|
        assert_equal('apple', tx[:foo])
        assert_equal(true, (tx.key? :foo))
        assert_equal([ [ :foo, 'apple' ] ], tx.to_a)
        assert_equal([ :foo ], tx.keys)
        assert_equal([ 'apple' ], tx.values)
        assert_equal(1, tx.length)
        assert_equal(false, tx.empty?)

        assert_instance_of(Time, tx.property(:foo, :created_time))
        assert_instance_of(Time, tx.property(:foo, :changed_time))
        assert_instance_of(Time, tx.property(:foo, :modified_time))
        assert_equal('SHA512', tx.property(:foo, :hash_type))
        assert_equal(Digest::SHA512.hexdigest('apple'), tx.property(:foo, :hash_value))
        assert_equal('banana', tx.property(:foo, 'bar'))

        assert_equal(true, (tx.property? :foo, :created_time))
        assert_equal(true, (tx.property? :foo, :changed_time))
        assert_equal(true, (tx.property? :foo, :modified_time))
        assert_equal(true, (tx.property? :foo, :hash_type))
        assert_equal(true, (tx.property? :foo, :hash_value))
        assert_equal(true, (tx.property? :foo, 'bar'))

        assert_alist = [
          [ :created_time,  proc{|n, v| assert_instance_of(Time, v, "name: #{n}") } ],
          [ :changed_time,  proc{|n, v| assert_instance_of(Time, v, "name: #{n}") } ],
          [ :modified_time, proc{|n, v| assert_instance_of(Time, v, "name: #{n}") } ],
          [ :hash_type,     proc{|n, v| assert_equal('SHA512', v, "name: #{n}") } ],
          [ :hash_value,    proc{|n, v| assert_equal(Digest::SHA512.hexdigest('apple'), v, "name: #{n}") } ],
          [ 'bar',          proc{|n, v| assert_equal('banana', v, "name: #{n}") } ]
        ]
        tx.each_property(:foo) do |name, value|
          assert(assert_pair = assert_alist.assoc(name), "name: #{name}")
          assert_pair[1].call(name, value)
          assert_alist.delete(assert_pair)
        end
        assert(assert_alist.empty?)

        assert_raise(NoMethodError) { tx[:foo] = 'melon' }
        assert_raise(NoMethodError) { tx.delete(:foo) }
        assert_raise(NoMethodError) { tx.delete_if{|key, value| value == 'apple' } }
        assert_raise(NoMethodError) { tx.set_property(:foo, 'baz', 'orange') }
        assert_raise(NoMethodError) { tx.delete_property(:foo, 'bar') }
        assert_raise(NoMethodError) { tx.clear }
        assert_raise(NoMethodError) { tx.commit }
        assert_raise(NoMethodError) { tx.rollback }
      }

      assert_raise(TransactionManager::NotWritableError) {
        @tman.transaction(false) {|tx|
          flunk('not to reach')
        }
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
