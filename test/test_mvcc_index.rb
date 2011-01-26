#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'fileutils'
require 'higgs/block'
require 'higgs/index'
require 'pp' if $DEBUG
require 'test/unit'

module Higgs::Test
  class MVCCIndexEditUtilsTest < Test::Unit::TestCase
    include Higgs::MVCCIndex::EditUtils

    def test_get_entry
      assert_equal('foo', get_entry(7, [ [ 7, 'foo' ] ]))
      assert_equal('foo', get_entry(8, [ [ 7, 'foo' ] ]))
      assert_equal(nil, get_entry(6, [ [ 7, 'foo' ] ]))

      assert_equal('foo', get_entry(8, [ [ 8, 'foo'], [ 7, 'bar' ], [ 6, 'baz' ] ]))
      assert_equal('bar', get_entry(7, [ [ 8, 'foo'], [ 7, 'bar' ], [ 6, 'baz' ] ]))
      assert_equal('baz', get_entry(6, [ [ 8, 'foo'], [ 7, 'bar' ], [ 6, 'baz' ] ]))

      assert_equal(nil, get_entry(7, [ [ 7, nil], [ 6, 'foo' ] ]))
      assert_equal('foo', get_entry(6, [ [ 7, nil], [ 6, 'foo' ] ]))
    end

    def test_put_entry_new
      entry_alist = put_entry(0, [], 'foo')
      assert_equal([ [ 1, 'foo' ], [ 0, nil ] ], entry_alist)
      assert_equal('foo', get_entry(1, entry_alist))
      assert_equal(nil, get_entry(0, entry_alist))
    end

    def test_put_entry_update
      entry_alist = put_entry(2, [ [ 1, 'foo' ], [ 0, nil ] ], 'bar')
      assert_equal([ [ 3, 'bar' ], [ 1, 'foo' ], [ 0, nil ] ], entry_alist)
      assert_equal('bar', get_entry(3, entry_alist))
      assert_equal('foo', get_entry(2, entry_alist))
      assert_equal('foo', get_entry(1, entry_alist))
      assert_equal(nil, get_entry(0, entry_alist))
    end
  end

  class MVCCIndexTest < Test::Unit::TestCase
    include Higgs

    def setup
      @idx = MVCCIndex.new
    end

    def dump_index
      puts caller[0]
      pp @idx
    end
    private :dump_index

    def test_change_number
      assert_equal(0, @idx.change_number)
      @idx.transaction{|cnum| @idx.succ! }
      assert_equal(1, @idx.change_number)
      @idx.transaction{|cnum| @idx.succ! }
      assert_equal(2, @idx.change_number)
      @idx.transaction{|cnum| @idx.succ! }
      assert_equal(3, @idx.change_number)

      dump_index if $DEBUG
    end

    def test_free_list
      @idx.transaction{|cnum|
        assert_equal(nil, @idx.free_fetch(512))
        @idx.free_store(0, 512)
        assert_equal(nil, @idx.free_fetch(512))
        @idx.succ!
      }
      dump_index if $DEBUG

      @idx.transaction{|cnum|
        assert_equal(0, @idx.free_fetch(512))
        assert_equal(nil, @idx.free_fetch(512))
      }
      dump_index if $DEBUG
    end

    def test_free_list_multi_size
      @idx.transaction{|cnum|
        @idx.free_store(0, 1024)
        @idx.free_store(3584, 8192)
        assert_equal(nil, @idx.free_fetch(1024))
        assert_equal(nil, @idx.free_fetch(8192))
        @idx.succ!
      }
      dump_index if $DEBUG

      @idx.transaction{|cnum|
        assert_equal(0, @idx.free_fetch(1024))
        assert_equal(nil, @idx.free_fetch(1024))
        assert_equal(3584, @idx.free_fetch(8192))
        assert_equal(nil, @idx.free_fetch(8192))
      }
      dump_index if $DEBUG
    end

    def test_free_list_multi_segment
      @idx.transaction{|cnum|
        @idx.free_store(0, 512)
        @idx.free_store(8192, 512)
        @idx.free_store(9782, 512)
        assert_equal(nil, @idx.free_fetch(512))
        @idx.succ!
      }
      dump_index if $DEBUG

      @idx.transaction{|cnum|
        assert_equal(0, @idx.free_fetch(512))
        assert_equal(8192, @idx.free_fetch(512))
        assert_equal(9782, @idx.free_fetch(512))
        assert_equal(nil, @idx.free_fetch(512))
      }
      dump_index if $DEBUG
    end

    def test_free_fetch_at
      @idx.transaction{|cnum|
        @idx.free_store(0, 512)
        @idx.free_store(8192, 512)
        @idx.free_store(9782, 512)
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(8192, @idx.free_fetch_at(8192, 512))
        dump_index if $DEBUG
        assert_equal(0, @idx.free_fetch(512))
        assert_equal(9782, @idx.free_fetch(512))
        assert_equal(nil, @idx.free_fetch(512))
      }
    end

    def test_index_entry
      @idx.transaction{|cnum|
        assert_equal(nil, @idx[cnum, 'foo'])
        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        dump_index if $DEBUG
        assert_equal(nil, @idx[cnum, 'foo'])
        assert_equal(0, @idx[cnum.succ, 'foo'])
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(0, @idx[cnum, 'foo'])
      }
    end

    def test_index_multi_entries
      @idx.transaction{|cnum|
        assert_equal(nil, @idx[cnum, 'foo'])
        assert_equal(nil, @idx[cnum, 'bar'])
        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        @idx[cnum, 'bar'] = 8192
        dump_index if $DEBUG
        assert_equal(nil, @idx[cnum, 'foo'])
        assert_equal(nil, @idx[cnum, 'bar'])
        assert_equal(0, @idx[cnum.succ, 'foo'])
        assert_equal(8192, @idx[cnum.succ, 'bar'])
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(0, @idx[cnum, 'foo'])
        assert_equal(8192, @idx[cnum, 'bar'])
      }
    end

    def test_key
      @idx.transaction{|cnum|
        assert_equal(false, @idx.key?(cnum, 'foo'))
        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        dump_index if $DEBUG
        assert_equal(false, @idx.key?(cnum, 'foo'))
        assert_equal(true, @idx.key?(cnum.succ, 'foo'))
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(true, @idx.key?(cnum, 'foo'))
      }
    end

    def test_keys
      @idx.transaction{|cnum|
        assert_equal([], @idx.keys(cnum))
        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        dump_index if $DEBUG
        assert_equal([], @idx.keys(cnum))
        assert_equal(%w[ foo ], @idx.keys(cnum.succ))
        @idx.succ!
      }

      @idx.transaction{|cnum|
        assert_equal(%w[ foo ], @idx.keys(cnum))
        dump_index if $DEBUG
        @idx[cnum, 'bar'] = 512
        @idx[cnum, 'baz'] = 1024
        dump_index if $DEBUG
        assert_equal(%w[ foo ], @idx.keys(cnum))
        assert_equal(%w[ foo bar baz ].sort, @idx.keys(cnum.succ).sort)
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(%w[ foo bar baz ].sort, @idx.keys(cnum).sort)
      }
    end

    def test_each_key
      @idx.transaction{|cnum|
        @idx.each_key(cnum) do |key|
          flunk('not to reach.')
        end

        dump_index if $DEBUG
        @idx[cnum, 'foo'] = 0
        dump_index if $DEBUG

        @idx.each_key(cnum) do |key|
          flunk('not to reach.')
        end

        expected_keys = %w[ foo ]
        @idx.each_key(cnum.succ) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end

        @idx.succ!
      }

      @idx.transaction{|cnum|
        expected_keys = %w[ foo ]
        @idx.each_key(cnum) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end

        dump_index if $DEBUG
        @idx[cnum, 'bar'] = 512
        @idx[cnum, 'baz'] = 1024
        dump_index if $DEBUG

        expected_keys = %w[ foo ]
        @idx.each_key(cnum) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end

        dump_index if $DEBUG
        expected_keys = %w[ foo bar baz ]
        @idx.each_key(cnum.succ) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end

        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        expected_keys = %w[ foo bar baz ]
        @idx.each_key(cnum) do |key|
          assert(expected_keys.delete(key), "key: #{key}")
        end
      }
    end

    def test_delete
      @idx.transaction{|cnum|
        assert_equal(nil, @idx.delete(cnum, 'foo'))
        @idx[cnum, 'foo'] = 0
        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal(0, @idx.delete(cnum, 'foo'))
        dump_index if $DEBUG

        assert_equal(%w[ foo ], @idx.keys(cnum))
        assert_equal(true, @idx.key?(cnum, 'foo'))
        assert_equal(0, @idx[cnum, 'foo'])

        @idx.succ!
      }

      @idx.transaction{|cnum|
        dump_index if $DEBUG
        assert_equal([], @idx.keys(cnum))
        assert_equal(false, @idx.key?(cnum, 'foo'))
        assert_equal(nil, @idx[cnum, 'foo'])
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
