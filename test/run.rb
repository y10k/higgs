#!/usr/local/bin/ruby

$: << File.join(File.dirname($0))
$: << File.join(File.dirname($0), '..', 'lib')

# for ident(1)
CVS_ID = '$Id$'

mask = //                       # any match
if ($0 == __FILE__) then
  if (mask_pattern = ARGV.shift) then
    mask = Regexp.compile(mask_pattern)
  end
end

test_dir, this_name = File.split(__FILE__)
Dir.foreach(test_dir) do |test_rb|
  case (test_rb)
  when this_name
    # skip
  when /^test_.*\.rb$/
    if (test_rb =~ mask) then
      puts "load #{test_rb}"
      require File.join(test_dir, test_rb)
    end
  end
end
