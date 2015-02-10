import pickle





#=====================================================================================================        
class Message(object):
    def __init__(self, command = None, options = None,  user = None, host = None):
        self.msg = dict()
        self.msg['command'] = command
        if(isinstance(options,list) or options == None): 
            self.msg['options'] = options
        else:
            self.msg['options'] = [options]
        self.msg['user'] = user
        self.cmd = command
        self.opt = options
        self.user = user
        self.host = host
        self.raw_msg = None
    
    def compose(self):
        self.raw_msg = pickle.dumps(self.msg) 
        return self.raw_msg
    
    def check_sanity(self, add_keys = None):
        std_keys = ['command','options','user']
        if(add_keys != None):
            std_keys.append(add_keys)
        for k in std_keys:
            if(k not in self.msg.keys()):
                raise ValueError("Key '%s' not present in message."%k)
            
    def decompose(self, msg):
        self.raw_msg = msg
        self.msg = pickle.loads(msg)
        self.check_sanity()
        self.cmd = self.msg['command']
        self.opt = self.msg['options']
        self.user = self.msg['user']
        return self.msg
    
#=====================================================================================================
class RetMessage(object):
    def __init__(self, server = None, status = None):
        self.msg = dict()
        if(server != None):
            self.scheduler_name = server.scheduler_name
            self.host = server.host
        self.status = status
        self.composed = None
    def compose(self):
        self.msg['host'] = self.host
        self.msg['status'] = self.status
        self.msg['scheduler_name'] = self.scheduler_name
        self.composed  = pickle.dumps(self.msg)
        return self.composed
    def decompose(self, msg):
        self.msg = pickle.loads(msg)
        self.host = self.msg['host']
        self.status = self.msg['status']
        self.scheduler_name = self.msg['scheduler_name']