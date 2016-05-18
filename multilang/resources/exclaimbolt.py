import storm


class ExclaimBolt(storm.BasicBolt):
    def process(self, tup):
        # open('/home/roy/output.txt', 'a').write(str(tup) + '\n')
        input = tup.values[1]
        storm.emit([tup.values[0], input + '!!!python!!!'])

ExclaimBolt().run()
