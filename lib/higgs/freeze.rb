# -*- coding: utf-8 -*-

class Object
  # freeze object tree.
  def higgs_deep_freeze
    unless (frozen?) then
      freeze
      for name in instance_variables
        instance_variable_get(name).higgs_deep_freeze
      end
    end

    self
  end
end

class Array
  # freeze object tree.
  def higgs_deep_freeze
    unless (frozen?) then
      super
      for i in self
        i.higgs_deep_freeze
      end
    end

    self
  end
end

class Hash
  # freeze object tree.
  def higgs_deep_freeze
    unless (frozen?) then
      super
      for k, v in self
        k.higgs_deep_freeze
        v.higgs_deep_freeze
      end
    end

    self
  end
end

class Struct
  # freeze object tree.
  def higgs_deep_freeze
    unless (frozen?) then
      super
      for i in self
        i.higgs_deep_freeze
      end
    end

    self
  end
end

class NilClass
  # no freeze immutable object.
  def higgs_deep_freeze
    self
  end
end

class TrueClass
  # no freeze immutable object.
  def higgs_deep_freeze
    self
  end
end

class FalseClass
  # no freeze immutable object.
  def higgs_deep_freeze
    self
  end
end

class Symbol
  # no freeze immutable object.
  def higgs_deep_freeze
    self
  end
end

class Module
  # no freeze immutable object.
  def higgs_deep_freeze
    self
  end
end

class Numeric
  # no freeze immutable object.
  def higgs_deep_freeze
    self
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
