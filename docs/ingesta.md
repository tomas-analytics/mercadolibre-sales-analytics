# 🔄 Proceso de ingesta

## 🎯 Objetivo

Procesar archivos de ventas de Mercado Libre de manera robusta y tolerante a cambios.

---

## ⚙️ Flujo de ingesta

```text
Archivo
↓
Detectar header
↓
Leer datos
↓
Normalizar columnas
↓
Mapear a esquema interno
↓
Validar columnas
↓
Tipar datos
↓
Agregar metadata
↓
Carga incremental
↓
Auditoría
```

---

## 🔍 Detección de header

El sistema analiza las primeras filas del archivo para detectar cuál contiene los nombres de columnas.

Criterio:

* mayor coincidencia con columnas esperadas

---

## 🔤 Normalización de columnas

Se aplican reglas:

* minúsculas
* sin tildes
* espacios → underscore
* eliminación de caracteres especiales

Ejemplo:

```text
"Fecha de venta" → "fecha_de_venta"
```

---

## 🔁 Mapeo de columnas

Se utiliza un archivo YAML:

```text
etl/config/column_mapping.yml
```

Permite mapear múltiples nombres a una columna estándar.

---

## ✅ Validación

Se verifican columnas obligatorias:

* sale_id
* sale_date
* sale_status
* quantity
* total_amount_ars

Si faltan → error

---

## 🔢 Tipado de datos

Conversión a tipos correctos:

* fechas
* numéricos
* strings

---

## 🔁 Carga incremental

Solo se insertan registros nuevos:

* basado en `sale_id`
* evita duplicados

---

## 🧾 Auditoría

Cada carga genera un registro en:

`raw_ml_loads`

Incluye:

* file_name
* file_hash
* loaded_at
* cantidad de registros
* estado de la carga

---

## 🚀 Beneficios

* tolerancia a cambios
* consistencia de datos
* trazabilidad
* robustez
