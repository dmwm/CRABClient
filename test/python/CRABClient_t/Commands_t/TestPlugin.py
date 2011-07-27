from JobType.BasicJobType import BasicJobType

class TestPlugin(BasicJobType):
    def run(self, config):
        return [],''

    def validateConfig(self,config):
        return (True, '')
