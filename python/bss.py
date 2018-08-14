class XdrFormatter(object):
    def __init__(self, fields : int):
        self.fields = fields
xdr = XdrFormatter(fields = [1,2,3,4])
print(xdr.fields)