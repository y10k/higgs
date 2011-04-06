# -*- coding: utf-8 -*-

class Object
  # freeze object tree.
  def higgs_deep_freeze
    for name in instance_variables
      instance_variable_get(name).higgs_deep_freeze
    end
    freeze

    self
  end
end

class Array
  # freeze object tree.
  def higgs_deep_freeze
    for i in self
      i.higgs_deep_freeze
    end

    super
  end
end

class Hash
  # freeze object tree.
  def higgs_deep_freeze
    for k, v in self
      k.higgs_deep_freeze
      v.higgs_deep_freeze
    end

    super
  end
end

class Struct
  # freeze object tree.
  def higgs_deep_freeze
    for i in self
      i.higgs_deep_freeze
    end

    super
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
