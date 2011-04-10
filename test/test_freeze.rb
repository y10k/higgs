#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'higgs/freeze'
require 'test/unit'

class HiggsDeepFreezeTest < Test::Unit::TestCase
  class ObjectData
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
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
