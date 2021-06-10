import os, re
import yaml

class YamlParser():

    def __init__(self):
        self.yaml = None

    def ImportFile(self, filename):

        with open(filename, "r") as inputfile:
            self.yaml = yaml.load(inputfile, Loader=yaml.FullLoader)

        # print self.yaml
        # print self.yaml['task']

    def Get(self, varname):

        # print "DEBUG - Requested variable {}".format(varname)

        if not varname in self.yaml['task']:
            print "[YamlParser] Warning: variable {} not found in file".format(varname)
            return None

        var = self.yaml['task'][varname]

        result = re.findall('\$\w*', str(var))
        for expr in result:
            # print "     found env expr {}".format(expr)
            value = os.environ[expr[1:]]
            # print "     expanding to {}".format(value)
            var = var.replace(expr, value)

        # print "  -  Value: {}".format(var)

        return var
