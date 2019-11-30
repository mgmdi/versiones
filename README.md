# Controlador de versiones

## Cómo ejecutar

### Configuración de un entorno virtual

Cree un nuevo entorno virtual en la carpeta del proyecto con python 3.6 o 3.7 por defecto:

```
virtualenv env --python=$(which python3.6)
```

Para activar el entorno:
```
source env/bin/activate
```

Para desactivar el entorno:
```
deactivate
```

Para instalar los paquetes necesarios:
```
pip install -r requirements.txt
```

### Ejecutar servidor de nombre

Asegúrese de activar el entorno virtual antes de continuar, luego coloque en una consola:

```
python src/main/python/NS.py
```

### Ejecutar servidor de versiones y cliente

Luego de ejecutar el servidor de nombre, coloque en dos consolas:

```
python src/main/python/versionController.py
```

y

```
python src/main/python/client.py
```
