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
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
