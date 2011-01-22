#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'fileutils'
require 'higgs/block'
require 'higgs/index'
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
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
