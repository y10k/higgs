#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/store'
require 'logger'
require 'test/unit'

module Higgs::Test
  class StoreTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @test_dir = 'store_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @name = File.join(@test_dir, 'foo')
      @st = Store.new(@name,
                      :logger => proc{|path|
                        logger = Logger.new(path, 1)
                        logger.level = Logger::DEBUG
                        logger
                      })
    end

    def teardown
      @st.shutdown unless @st.shutdown?
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end

    def test_fetch_and_store
      @st.transaction{|tx|
        assert_equal(nil, tx[:foo])
        assert_equal(nil, tx['bar'])
        assert_equal(nil, tx[0])

        assert_equal(false, (tx.key? :foo))
        assert_equal(false, (tx.key? 'bar'))
        assert_equal(false, (tx.key? 0))

        tx[:foo] = :HALO
        tx['bar'] = "Hello world.\n"
        tx[0] = nil

        assert_equal(:HALO,            tx[:foo])
        assert_equal("Hello world.\n", tx['bar'])
        assert_equal(nil,              tx[0])

        assert_equal(true, (tx.key? :foo))
        assert_equal(true, (tx.key? 'bar'))
        assert_equal(true, (tx.key? 0))
      }

      @st.transaction{|tx|
        assert_equal(:HALO,            tx[:foo])
        assert_equal("Hello world.\n", tx['bar'])
        assert_equal(nil,              tx[0])

        assert_equal(true, (tx.key? :foo))
        assert_equal(true, (tx.key? 'bar'))
        assert_equal(true, (tx.key? 0))
      }
    end

    def test_string_only
      @st.transaction{|tx|
        assert_equal(nil, tx.property(:foo, :string_only))
        tx[:foo] = 0
      }

      @st.transaction{|tx|
        assert_equal(false, tx.property(:foo, :string_only))
        assert_equal(0, tx[:foo])
        tx.set_property(:foo, :string_only, true)
        assert_equal(0, tx[:foo])
      }

      @st.transaction{|tx|
        assert_equal(true, tx.property(:foo, :string_only))
        assert_equal(Marshal.dump(0), tx[:foo])
        tx[:foo] = "Hello world.\n"
      }

      @st.transaction{|tx|
        assert_equal(true, tx.property(:foo, :string_only))
        assert_equal("Hello world.\n", tx[:foo])
      }
    end

    def test_string_only_TypeError_cant_convert_into_String
      @st.transaction{|tx|
        tx[:foo] = 0
        tx.set_property(:foo, :string_only, true)
        assert_raise(TypeError) { tx.commit }
        tx.rollback
      }
    end

    def test_rollback
      @st.transaction{|tx|
        tx[:foo] = %w[ apple banana orange ]
      }

      @st.transaction{|tx|
        fruits = tx[:foo]
        assert_equal(%w[ apple banana orange ], fruits)
        assert_equal('orange', fruits.pop)
        assert_equal(%w[ apple banana ], fruits)
        tx[:foo] = fruits
        tx.rollback
        assert_equal(%w[ apple banana orange ], tx[:foo])
      }

      @st.transaction{|tx|
        assert_equal(%w[ apple banana orange ], tx[:foo])
      }
    end

    def test_replica_problem
      @st.transaction{|tx|
        tx[:foo] = 'a'
        tx[:bar] = tx[:foo]

        assert_equal('a', tx[:foo])
        assert_equal('a', tx[:bar])
        assert_same(tx[:foo], tx[:bar])

        tx[:foo].succ!
        assert_equal('b', tx[:foo])
        assert_equal('b', tx[:bar])
        assert_same(tx[:foo], tx[:bar])
      }

      @st.transaction{|tx|
        assert_equal('b', tx[:foo])
        assert_equal('b', tx[:bar])
        assert_not_same(tx[:foo], tx[:bar])

        tx[:foo].succ!
        assert_equal('c', tx[:foo])
        assert_equal('b', tx[:bar])
        assert_not_same(tx[:foo], tx[:bar])
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
