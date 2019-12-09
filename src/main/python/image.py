
from PIL import Image

try:  
    img  = Image.open("test2.jpg")  
    datos = Image.Image.getdata(img)
    print(datos)
except IOError: 
    print("error")