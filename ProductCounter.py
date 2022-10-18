import numpy as np
import cv2 as cv
import os
import tensorflow.keras.models as models

from FridgeContentDetector import *

class FridgeContentCounter():
    def __init__(self, model_path = './sodas_detector_prot', demo_images_dir = "../test3_images", labels = ['fresca lata 355 ml', 'sidral mundet lata 355 ml', 'fresca botella de plastico 600 ml', 'fuze tea durazno 600 ml', 'power ade mora azul botella de plastico 500 ml', 'delaware punch lata 355 ml', 'vacio', 'del valle durazno botella de vidrio 413 ml', 'sidral mundet botella de plastico 600 ml', 'coca cola botella de plastico 600 ml', 'power ade mora azul lata 453 ml', 'coca cola lata 355 ml', 'producto no oficial'], ean = ["7501055365470", "7501055363162", "7501055303786", "7501055317875", "7501055329267", "7501055365609", "0", "3223905201", "7501055339983", "75007614", "7501055370986", "7501055361540", "-1"], alfa = 0.4, beta = 0.6, thresh = 0.65):
        self.demo_images_dir = demo_images_dir
        self.labels = labels
        self.prev_pred = np.zeros((8, len(self.labels) - 1))
        self.prev_pred[:, 6] = 1
        self.alfa = alfa
        self.beta = beta
        self.thresh = thresh
        self.model = models.load_model(model_path)
        self.unlable = {}
        self.ean = ean
    
    def show_count_result(self, label, max_pred, cell_num, cell):
        pred_lbl = label 
        pred_pred =  str(max_pred*100) + "% "
        cel_cnt = "Cell: " + str(cell_num)
        font = cv.FONT_HERSHEY_SIMPLEX
        org1 = (1, 15)
        org2 = (1, 23)
        org3 = (1, 31)
        fontScale = 0.2
        color = (255, 0, 0)
        thickness = 1
        cell = cv.putText(cell, pred_lbl, org1, font, fontScale, color, thickness)
        cell = cv.putText(cell, pred_pred, org2, font, fontScale, color, thickness)
        cell = cv.putText(cell, cel_cnt, org3, font, fontScale, color, thickness)
        cv.imshow("CellPred", cell)
        cv.waitKey(0)
    
    def get_content_count(self, raw_image, fridge_rows_count = 2, fridge_columns_count = 4, output_shape=(150,420), verbose = False):
        fridge_content_detector = FridgeContentDetector()
        
        content_count = {}
        for org_label in self.labels:
            content_count[org_label] = 0
            
        cell_num = 1
        
        ean_count = {}
        
        for num in self.ean:
            ean_count[num] = 0

        try:
            content_cells = fridge_content_detector.get_fridge_cells(raw_image, fridge_rows_count, fridge_columns_count, output_shape)
            
            
            for cell in content_cells:
                # gray_cell = cv.cvtColor(cell, cv.COLOR_BGR2GRAY)
                cell = cell/255
                expanded_cell = np.expand_dims(cell, axis = 0)
                # expanded_cell = np.expand_dims(expanded_cell, axis = 3)
                pred = self.model.predict(expanded_cell)[0]
                
                sum_preds = self.alfa * self.prev_pred[cell_num - 1] + self.beta * pred
                
                self.prev_pred[cell_num - 1] = pred
                
                max_pred = np.amax(sum_preds)
                
                if max_pred < self.thresh:
                    label = self.labels[-1]
                else:
                    label = self.labels[int(np.where(sum_preds == max_pred)[0][0])]
                
                
                if verbose:
                    self.show_count_result(label, max_pred, cell_num, cell)
                cell_num += 1
                
                content_count[label] += 1
                
        except FridgeNotFoundException:
            
            for i in range(8):
                sum_preds = self.prev_pred[i]

                max_pred = np.amax(sum_preds)
                
                if max_pred < self.thresh:
                    label = self.labels[-1]
                else:
                    label = self.labels[int(np.where(sum_preds == max_pred)[0][0])]
                
                content_count[label] += 1
        
        i = 0
        for label in content_count:
            ean_count[self.ean[i]] = content_count[label]
            i += 1
        
        return ean_count
            
    def run_demo(self, verbose = True):
        for image_name in os.listdir(self.demo_images_dir):
            if image_name.endswith(".jpg"):
                image_dir = os.path.join(self.demo_images_dir, image_name)
                image = cv.imread(image_dir)
                height, width = image.shape[:2]
                reduced_dims = ( width//2 , height//2 )
                image = cv.resize(image, reduced_dims)
                content = self.get_content_count(image, verbose = verbose)
                print(content)
                
        cv.destroyAllWindows()
                
if __name__ == "__main__":
    product_counter = FridgeContentCounter()
    product_counter.run_demo(verbose = True)