import pickle 
enc = pickle.load(open('D:/ProjectZZZ/models/encoders_model_lightgbm_v1.pkl', 'rb')) 
print(type(enc)) 
print(enc.keys() if isinstance(enc, dict) else 'NOT A DICT') 
