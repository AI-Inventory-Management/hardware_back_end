from InitializationMessageUploader import InitializationMessageUploader as IniMU
from MessageUploader import MessageUploader as ContMU

import json
from threading import Event

class RIICOMain ():
    def __init__(self, wait_time = 1800):
        self.initial_uploader = IniMU()
        self.uploader = ContMU()
        self.wait_time = wait_time
        
    def send_initial(self, verbose = False):
        had_data = self.initial_uploader.obtain_store_data()
        if had_data:
            self.initial_uploader.upload_message(verbose = verbose)
            self.initial_uploader.build_data_file(verbose = verbose)
            if verbose:
                print("First message sent.")
        else:
            if verbose:
                print("First message already sent.")
        
    def update_store_info (self, verbose = False):
        with open("./data/store_data.json") as f:
            data = json.load(f)
            f.close()
            store_id = data["store_id"]
            
            self.uploader.set_store_id(store_id = store_id)
            
            if verbose:
                print("Store id updated")
    
    def run(self, verbose = False, time_range = (0, 30)):
        self.send_initial(verbose = verbose)
        self.update_store_info(verbose = verbose)
        
        message_wait = 0
        
        while True:
            if verbose :
                print("Message wait: " + str(message_wait))
                print("Image capture wait: " + str((self.wait_time - message_wait)/60) + "min")
                print("Total wait: " + str(self.wait_time))
                
            Event().wait(self.wait_time - message_wait)
            self.uploader.capture_image()
            self.uploader.build_message()
            message_wait = self.uploader.upload_message(time_range = time_range, verbose = verbose)
            
    def run_demo(self):
        self.run(verbose = True, time_range = (0, 0.1))
        
if __name__ == '__main__':
    main = RIICOMain(wait_time = 30)
    main.run_demo()            