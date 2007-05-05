#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/store'
require 'test/unit'

module Higgs::StoreTest
  # for ident(1)
  CVS_ID = '$Id$'

  class StoreTest < Test::Unit::TestCase
    include Higgs

    def setup
      @test_dir = 'store_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @name = File.join(@test_dir, 'foo')
      @st = Store.new(@name)
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
  end
end
