#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'higgs/freeze'
require 'test/unit'

class HiggsDeepFreezeTest < Test::Unit::TestCase
  class ObjectData
    def initialize(foo=nil, bar=nil)
      @foo = foo
      @bar = bar
    end

    attr_accessor :foo
    attr_accessor :bar
  end

  StructData = Struct.new(:foo, :bar)

  def test_freeze_object
    o = ObjectData.new
    o.higgs_deep_freeze
    assert_equal(true, o.frozen?)
  end

  def test_freeze_array
    a = []
    a.higgs_deep_freeze
    assert_equal(true, a.frozen?)
  end

  def test_freeze_hash
    h = []
    h.higgs_deep_freeze
    assert_equal(true, h.frozen?)
  end

  def test_freeze_struct
    s = StructData.new
    s.higgs_deep_freeze
    assert_equal(true, s.frozen?)
  end

  def test_no_freeze_nil
    n = nil
    n.higgs_deep_freeze
    assert_equal(false, n.frozen?)
  end

  def test_no_freeze_true
    t = true
    t.higgs_deep_freeze
    assert_equal(false, t.frozen?)
  end

  def test_no_freeze_false
    f = false
    f.higgs_deep_freeze
    assert_equal(false, f.frozen?)
  end

  def test_no_freeze_symbol
    s = :foo
    s.higgs_deep_freeze
    assert_equal(false, s.frozen?)
  end

  def test_no_freeze_module
    m = Module.new
    m.higgs_deep_freeze
    assert_equal(false, m.frozen?)
  end

  def test_no_freeze_class
    c = Class.new
    c.higgs_deep_freeze
    assert_equal(false, c.frozen?)
  end

  def test_no_freeze_fixnum
    n = 0
    assert_instance_of(Fixnum, n)
    n.higgs_deep_freeze
    assert_equal(false, n.frozen?)
  end

  def test_no_freeze_bignum
    n = 10**23
    assert_instance_of(Bignum, n)
    n.higgs_deep_freeze
    assert_equal(false, n.frozen?)
  end

  def test_no_freeze_float
    n = 1.0
    assert_instance_of(Float, n)
    assert_equal(false, n.frozen?)
  end

  def test_freeze_object_tree
    k = Object.new
    o_tree = [ 
      { :object => ObjectData.new([ 'in_object' ], :bar),
        :array => [ 'in_array' ],
        :hash => { k => [ 'in_hash' ] },
        :struct => StructData.new([ 'in_struct' ], :bar),
        :nil => nil,
        :true => true,
        :false => false,
        :symbol => :foo,
        :module => Module.new,
        :class => Class.new,
        :fixnum => 0,
        :bignum => 10**23,
        :float => 1.0
      }
    ]
    o_tree.higgs_deep_freeze

    assert_equal(true, o_tree.frozen?)
    assert_equal(true, o_tree[0].frozen?)
    assert_equal(true, o_tree[0][:object].frozen?)
    assert_equal(true, o_tree[0][:object].foo.frozen?)
    assert_equal(true, o_tree[0][:object].foo[0].frozen?)
    assert_equal(true, o_tree[0][:array].frozen?)
    assert_equal(true, o_tree[0][:array][0].frozen?)
    assert_equal(true, o_tree[0][:hash].frozen?)
    assert_equal(true, o_tree[0][:hash].keys[0].frozen?)
    assert_equal(true, o_tree[0][:hash].values[0].frozen?)
    assert_equal(true, o_tree[0][:hash].values[0][0].frozen?)
    assert_equal(true, o_tree[0][:struct].frozen?)
    assert_equal(true, o_tree[0][:struct].foo.frozen?)
    assert_equal(true, o_tree[0][:struct].foo[0].frozen?)
    assert_equal(false, o_tree[0][:nil].frozen?)
    assert_equal(false, o_tree[0][:true].frozen?)
    assert_equal(false, o_tree[0][:false].frozen?)
    assert_equal(false, o_tree[0][:symbol].frozen?)
    assert_equal(false, o_tree[0][:module].frozen?)
    assert_equal(false, o_tree[0][:class].frozen?)
    assert_instance_of(Fixnum, o_tree[0][:fixnum])
    assert_equal(false, o_tree[0][:fixnum].frozen?)
    assert_instance_of(Bignum, o_tree[0][:bignum])
    assert_equal(false, o_tree[0][:bignum].frozen?)
    assert_instance_of(Float, o_tree[0][:float])
    assert_equal(false, o_tree[0][:float].frozen?)
  end

  def test_object_cycle
    a = ObjectData.new
    b = StructData.new
    a.foo = b
    b.foo = a
    a.higgs_deep_freeze
    assert_equal(true, a.frozen?)
    assert_equal(true, b.frozen?)
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
