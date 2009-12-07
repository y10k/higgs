#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'fileutils'
require 'higgs/block'
require 'higgs/index'
require 'test/unit'

module Higgs::Test
  class IndexChangeNumberTest < Test::Unit::TestCase
    include Higgs

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

  class IndexIdentitiesTest < Test::Unit::TestCase
    include Higgs

    def setup
      @idx = Index.new
    end

    def test_identity
      @idx[:foo] = 0
      assert_equal('foo', @idx.identity(:foo))
      @idx[:foo] = 1            # overwrite index entry
      assert_equal('foo', @idx.identity(:foo))
    end

    def test_identity_not_defined
      assert_nil(@idx.identity(:foo))
    end

    def test_identity_dup
      @idx[:foo] = 0
      @idx['foo'] = 1
      assert_equal('foo', @idx.identity(:foo))
      assert_equal('foo.a', @idx.identity('foo'))
    end

    def test_delete
      @idx[:foo] = 0
      @idx['foo'] = 1
      @idx.delete(:foo)
      assert_equal(nil, @idx.identity(:foo))
      assert_equal('foo.a', @idx.identity('foo'))
      @idx['foo'] = 2           # overwrite index entry
      assert_equal('foo.a', @idx.identity('foo'))
    end
  end

  class IndexLoadSaveTest < Test::Unit::TestCase
    include Higgs

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
      assert_equal(2048, j.eoa)
      assert_equal(0, j.free_fetch(512))
      assert_equal(1024, j[:foo])
      assert_equal('foo', j.identity(:foo))
    end

    def test_migration
      index_data_0_0 = {
        :version => [ 0, 0 ],
        :change_number => 0,
        :eoa => 1024,
        :free_lists => { 512 => [ 512 ] },
        :index => { :foo => 0 }
      }
      File.open(@path, 'w') {|w|
        w.binmode
        w.set_encoding(Encoding::ASCII_8BIT)
        Block.block_write(w, Index::MAGIC_SYMBOL, Marshal.dump(index_data_0_0))
      }

      i = Index.new
      i.storage_id = '68c6b76688d84b4d72856d8f589a5551'
      i.load(@path)

      h = i.to_h
      assert_equal([ Index::MAJOR_VERSION, Index::MINOR_VERSION ], h[:version])
      assert_equal(0, h[:change_number])
      assert_equal(1024, h[:eoa])
      assert_equal({ 512 => [ 512 ] }, h[:free_lists])
      assert_equal({ :foo => [ 'foo', 0 ] }, h[:index], 'index format version: 0.1')
      assert_equal({ 'foo' => :foo }, h[:identities], 'index format version: 0.1')
      assert_equal('68c6b76688d84b4d72856d8f589a5551', h[:storage_id], 'index format version: 0.2')

      assert_equal(0, i.change_number)
      assert_equal(1024, i.eoa)
      assert_equal(512, i.free_fetch(512))
      assert_equal(0, i[:foo])
      assert_equal('foo', i.identity(:foo))
      assert_equal('68c6b76688d84b4d72856d8f589a5551', i.storage_id)
    end

    def test_migration_RuntimeError_unsupported_version
      index_data_1_0 = {
        :version => [ 1, 0 ],
        :change_number => 0,
        :eoa => 1024,
        :free_lists => { 512 => [ 512 ] },
        :index => { :foo => 0 }
      }
      File.open(@path, 'w') {|w|
        w.binmode
        w.set_encoding(Encoding::ASCII_8BIT)
        Block.block_write(w, Index::MAGIC_SYMBOL, Marshal.dump(index_data_1_0))
      }

      i = Index.new
      assert_raise(RuntimeError) {
        i.load(@path)
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
