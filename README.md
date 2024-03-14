# LEASE STATUS UPDATER

## Descripcion
Diariamente va a la base de datos transaccional y verifica los que las operaciones *Active* ya hayan terminado de pagar todos sus plazos y les cambia el status a *Terminated* o *Concluded*

### Consideraciones
Este codigo se encuentra en la consola de AWS, configurado por medio de un EventBridge que se encarga de levantar una funcion Lambda todos los dias 14 de cada mes (Modificable). 
Esta funcion Lambda orquesta un Job de Glue donde se corre el cambio de status.
En el repositorio se encuentra dividido por archivo de clase, driver y templates.
En la consola se encuentra todo en el mismo driver 
