#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/index'
require 'test/unit'

module Higgs::Test
  class IndexChangeNumberTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @idx = Index.new
    end

    def test_change_number
      assert_equal(0, @idx.change_number)
      @idx.succ!
      assert_equal(1, @idx.change_number)
      @idx.succ!
      assert_equal(2, @idx.change_number)
      @idx.succ!
      assert_equal(3, @idx.change_number)
      @idx.succ!
      assert_equal(4, @idx.change_number)
      @idx.succ!
      assert_equal(5, @idx.change_number)
      @idx.succ!
      assert_equal(6, @idx.change_number)
      @idx.succ!
      assert_equal(7, @idx.change_number)
      @idx.succ!
      assert_equal(8, @idx.change_number)
      @idx.succ!
      assert_equal(9, @idx.change_number)
      @idx.succ!
      assert_equal(10, @idx.change_number)
    end
  end

  class IndexFreeListTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @idx = Index.new
    end

    def test_free_single
      assert_equal(nil, @idx.free_fetch(512))
      @idx.free_store(0, 512)
      assert_equal(0,   @idx.free_fetch(512))
      assert_equal(nil, @idx.free_fetch(512))
    end

    def test_free_multi_size
      @idx.free_store(0, 1024)
      @idx.free_store(3584, 8192)
      assert_equal(nil,  @idx.free_fetch(512))
      assert_equal(0,    @idx.free_fetch(1024))
      assert_equal(3584, @idx.free_fetch(8192))
    end

    def test_free_multi_segment
      @idx.free_store(0,    512)
      @idx.free_store(8192, 512)
      @idx.free_store(9782, 512)
      assert_equal(0,    @idx.free_fetch(512))
      assert_equal(8192, @idx.free_fetch(512))
      assert_equal(9782, @idx.free_fetch(512))
      assert_equal(nil,  @idx.free_fetch(512))
    end

    def test_free_fetch_at
      @idx.free_store(0,    512)
      @idx.free_store(8192, 512)
      @idx.free_store(9782, 512)
      assert_equal(0,    @idx.free_fetch_at(0, 512))
      assert_equal(8192, @idx.free_fetch_at(8192, 512))
      assert_equal(9782, @idx.free_fetch_at(9782, 512))
      assert_equal(nil,  @idx.free_fetch(512))
    end
  end

  class IndexTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @idx = Index.new
    end

    def test_single_entry
      assert_equal(nil, @idx['foo'])
      @idx['foo'] = 0
      assert_equal(0, @idx['foo'])
    end

    def test_multi_entry
      assert_equal(nil, @idx['foo'])
      assert_equal(nil, @idx['bar'])
      @idx['foo'] = 0
      @idx['bar'] = 8192
      assert_equal(0,    @idx['foo'])
      assert_equal(8192, @idx['bar'])
    end

    def test_key
      assert_equal(false, (@idx.key? 'foo'))
      @idx['foo'] = 0
      assert_equal(true, (@idx.key? 'foo'))
    end

    def test_keys
      assert_equal([], @idx.keys)
      @idx['foo'] = 0
      assert_equal(%w[ foo ].sort, @idx.keys.sort)
      @idx['bar'] = 512
      assert_equal(%w[ foo bar ].sort, @idx.keys.sort)
      @idx['baz'] = 1024
      assert_equal(%w[ foo bar baz ].sort, @idx.keys.sort)
    end

    def test_each_key
      assert_equal(@idx, @idx.each_key{|key| assert_fail('not to reach') })

      @idx['foo'] = 0
      expected_keys = %w[ foo ]
      @idx.each_key do |key|
        assert(expected_keys.delete(key), "key: #{key}")
      end
      assert_equal([], expected_keys)

      @idx['bar'] = 512
      expected_keys = %w[ foo bar ]
      @idx.each_key do |key|
        assert(expected_keys.delete(key), "key: #{key}")
      end
      assert_equal([], expected_keys)

      @idx['baz'] = 1024
      expected_keys = %w[ foo bar baz ]
      @idx.each_key do |key|
        assert(expected_keys.delete(key), "key: #{key}")
      end
      assert_equal([], expected_keys)
    end

    def test_delete
      assert_equal(nil, @idx.delete('foo'))
      @idx['foo'] = 0
      assert_equal(0, @idx.delete('foo'))
    end
  end

  class IndexLoadSaveTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @path = 'test.idx'
    end

    def teardown
      FileUtils.rm_f(@path)
    end

    def test_save_load
      i = Index.new
      i.succ!
      i.free_store(0, 512)
      i[:foo] = 1024
      i.eoa = 2048
      i.save(@path)

      j = Index.new
      j.load(@path)
      assert_equal(1, j.change_number)
      assert_equal(0, j.free_fetch(512))
      assert_equal(1024, j[:foo])
      assert_equal(2048, j.eoa)
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
