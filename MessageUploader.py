from datetime import datetime
import cv2 as cv
import numpy as np
import os
from threading import Event
import requests

from ProductCounter import FridgeContentCounter

class MessageUploader ():
    def __init__(self, image = [], store_id = "1",  demo_images_dir = "../test5_images"):
        self.store_id = "41"
        self.image = image
        self.demo_images_dir = demo_images_dir
        self.message = {}
        self.severs_handler_endpoint = "http://192.168.195.106:7000/constant_messages"
        self.fridge = FridgeContentCounter()
    
    def set_store_id(self, store_id):
        self.store_id = store_id
    
    def capture_image(self):
        camera = cv.VideoCapture(2)
        res, self.image = camera.read()
    
    def randomize_upload_time(self, time_range = (0, 30)):
        min_time = time_range[0] * 60
        max_time = time_range[1] * 60
        
        return np.random.randint(min_time, max_time)
    
    def build_message(self):
        content_count = self.fridge.get_content_count(self.image)
        dt = datetime.now()
        timestamp = datetime.timestamp(dt)
        
        #self.message = "TEMP_MESSAGE\n" + self.store_id + "\n" + str(content_count) + "\n" + str(timestamp)
        self.message["store_id"] = self.store_id
        self.message["content_count"] = content_count
        self.message["timestamp"] = str(timestamp)        

    def upload_message(self, time_range = (0, 30), verbose = False):
        wait_time = self.randomize_upload_time(time_range)
        if verbose:
            print("wait time is:")
            print(wait_time)
        Event().wait(wait_time)
        res = requests.post(self.severs_handler_endpoint, json=self.message)
        if res.ok:
            print("data sended to server succesfully")
            print(res.json())
        print(self.message)
        return(wait_time)

    def upload_test_mesage(self):
        '''method that will be used to send dummy data to test server connection
        PLEASE DO NOT USE THIS IN PRODUCTION'''
        content_count = {'fresca lata 355 ml':4, 'sidral mundet lata 355 ml': 1}
        dt = datetime.now()
        timestamp = datetime.timestamp(dt)
        
        #self.message = "TEMP_MESSAGE\n" + self.store_id + "\n" + str(content_count) + "\n" + str(timestamp)
        self.message["store_id"] = self.store_id
        self.message["content_count"] = content_count
        self.message["timestamp"] = str(timestamp)
        self.upload_message(verbose=True, time_range=(0,0.5))        
    
    def run_demo(self, verbose = False):
        for image_name in os.listdir(self.demo_images_dir):
            if image_name.endswith(".jpg"):
                image_dir = os.path.join(self.demo_images_dir, image_name)
                image = cv.imread(image_dir)
                height, width = image.shape[:2]
                reduced_dims = ( width//2 , height//2 )
                image = cv.resize(image, reduced_dims)
                self.image = image
                self.build_message()
                self.upload_message(time_range = (0, 1), verbose = verbose)
                if verbose:
                    cv.imshow("Imagen Actual", self.image)
                    cv.waitKey(0)
        
        cv.destroyAllWindows()
        
    def run_camera_demo(self):
        while True:
            self.capture_image()
            self.build_message()
            self.upload_message(time_range = (0,0.1), verbose = True)
            cv.imshow("Foto", self.image)
            pressed_key = cv.waitKey(0)
            if pressed_key == 0x15:
                # if user pressed q
                break
            #Event().wait(30)
            
if __name__ == "__main__":
    uploader = MessageUploader()
    #uploader.run_camera_demo()
    uploader.upload_test_mesage()