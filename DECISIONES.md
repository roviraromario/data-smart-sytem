# Decisiones Técnicas — DataMart S.A.S.

## 1. Modelo del repositorio analítico

Se usó una arquitectura de tres capas en PostgreSQL:

- **Bronze** (`bronze.online_retail_raw`): copia exacta de los CSV sin modificar. Todos los campos son TEXT para no perder ningún dato.
- **Silver** (`silver.online_retail`): datos limpios con tipos correctos (fechas en UTC, precios como NUMERIC, cantidades como INTEGER). Aquí se separan ventas de devoluciones.
- **Gold** (`gold.daily_product_sales`): totales diarios por producto y país. Es la tabla que responde las preguntas de negocio rápido.
- **Audit** (`audit.rejected_rows`): registros rechazados con el motivo del error.

**Por qué este diseño:** permite separar el problema de "guardar los datos" del problema de "limpiarlos" y del problema de "analizarlos". Si hay un error en la limpieza, Bronze siempre tiene los datos originales para volver a empezar.

## 2. Casos ambiguos resueltos

### Transacciones sin Customer ID
**Decisión:** se conservan con `customer_id = NULL`.  
**Razón:** representan compras de clientes anónimos (guest checkout), que son válidas en e-commerce. Descartarlas significaría perder 135.080 transacciones y más de $1.4 millones en revenue.

### Descripciones variantes del mismo producto
**Decisión:** se normalizan a mayúsculas y espacios simples con `UPPER(TRIM(...))`.  
**Ejemplo:** `Candle Holder White`, `CANDLE HOLDER WHITE` y `candle holder white` quedan como `CANDLE HOLDER WHITE`.

### Registros duplicados entre los dos datasets
**Decisión:** si una transacción aparece en ambos archivos (misma factura, producto, cantidad, fecha, precio, cliente y país), se conserva la del archivo diario `data.csv` y se descarta la del histórico.  
**Razón:** el archivo diario es la fuente oficial más reciente.

## 3. Idempotencia del DAG

Ejecutar el DAG dos veces el mismo día produce el mismo resultado porque:

- **Bronze:** antes de insertar, se borra la partición de la fuente (`DELETE WHERE source_name = ...`). Todo en una sola transacción.
- **Silver y Gold:** se hace `TRUNCATE` de la tabla y luego `INSERT`. Nunca hay duplicados.
- **DAG:** tiene `max_active_runs=1` (no corre en paralelo) y `catchup=False` (no recupera ejecuciones pasadas).

## 4. Categorías de productos

Como la API de productos es opcional, se asignó categoría mediante palabras clave en la descripción:

- `HOGAR`: candle, cup, jar, lantern, mug, vase...
- `ROPA`: coat, dress, hat, jacket, scarf, sock...
- `DEPORTES`: bicycle, football, gym, sport, tennis
- `PAPELERIA`: book, card, notebook, paper, pen
- `ELECTRONICA`: battery, camera, mouse, phone, usb
- `SIN_CATEGORIA`: cualquier descripción que no coincide

**Nota:** el 75% quedó como `SIN_CATEGORIA` porque el catálogo no tiene categorías propias.

## 5. Rechazo de registros

Se rechazan y guardan en `audit.rejected_rows` los registros con:
- Precio unitario cero, negativo o no numérico.
- En la carga verificada: **2.517 rechazos** de 1.078.259 filas totales (0.23%).

## 6. Manejo de fechas

- Las fechas sin zona horaria se interpretan como UTC.
- `data.csv` usa formato mes/día/año (`12/1/2010`).
- El archivo histórico también usa mes/día/año, detectado por el nombre de columna `TransactionNo`.
- Todas las fechas se convierten a `TIMESTAMPTZ` en UTC antes de guardarlas.
