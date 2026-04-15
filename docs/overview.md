# 📊 MercadoLibre Sales Analytics

## 🎯 Objetivo del proyecto

Este proyecto tiene como objetivo construir una solución analítica end-to-end para procesar, modelar y visualizar datos de ventas exportados desde Mercado Libre.

El sistema permite que un usuario cargue archivos Excel/CSV de ventas, los cuales son procesados automáticamente mediante un pipeline de datos que incluye:

* Ingesta y normalización de datos con Python
* Almacenamiento en BigQuery (capa RAW)
* Transformaciones analíticas con dbt
* Visualización mediante Streamlit

El objetivo principal es simular un entorno real de trabajo como Analista de Datos / Analytics Engineer, aplicando buenas prácticas de ingeniería de datos.

---

## 🧠 Problema a resolver

Los archivos exportados desde Mercado Libre presentan múltiples desafíos:

* La fila de encabezado no es fija
* El orden de columnas cambia
* Aparecen y desaparecen columnas según la versión
* La estructura económica evoluciona con el tiempo

Esto hace que un análisis directo en Excel o herramientas BI sea frágil e inconsistente.

---

## 💡 Solución propuesta

Se implementa un pipeline robusto que:

1. Detecta dinámicamente la estructura del archivo
2. Normaliza nombres de columnas
3. Mapea columnas a un esquema interno estándar
4. Valida datos críticos
5. Carga información en un Data Warehouse
6. Modela los datos en formato analítico (star schema)
7. Expone métricas mediante dashboards interactivos

---

## 🏗️ Arquitectura del sistema

El proyecto sigue una arquitectura en capas:

### 1. Ingesta (Python)

* Lectura de archivos Excel/CSV
* Detección de header
* Normalización de columnas
* Validación de esquema
* Tipado de datos
* Carga incremental

### 2. Data Warehouse (BigQuery)

* Tabla RAW con datos normalizados
* Tabla de auditoría de cargas

### 3. Transformaciones (dbt)

* STAGING: limpieza y tipado
* CORE: modelo dimensional
* MARTS: tablas analíticas listas para consumo

### 4. Visualización (Streamlit)

* Carga de archivos
* Dashboard interactivo
* KPIs y análisis exploratorio

---

## 📦 Estructura del proyecto

```text
mercadolibre-sales-analytics/
├── app/
├── etl/
├── transform/
├── tests/
├── docs/
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

## 🔑 Decisiones clave de diseño

### 1. Separación entre fuente y modelo

Los nombres de columnas originales (en español) son mapeados a nombres internos en inglés.

Ejemplo:

* "Fecha de venta" → `sale_date`
* "Ingresos (ARS)" → `product_revenue_ars`

Esto permite:

* estabilidad del sistema
* independencia del formato de entrada
* escalabilidad a nuevas fuentes de datos

---

### 2. Grano de datos

Cada fila representa una venta:

**1 fila = 1 venta**

---

### 3. Clave de negocio

`sale_id` (derivado de "# de venta")

---

### 4. Carga incremental

Solo se insertan nuevas ventas en base a `sale_id`.

---

### 5. Manejo de cambios en archivos

El sistema es tolerante a:

* cambios en el orden de columnas
* cambios en nombres
* aparición de nuevas columnas

---

## 🚀 Tecnologías utilizadas

* Python
* Pandas
* BigQuery
* dbt
* Streamlit
* YAML

---

## 📈 Objetivo profesional

Este proyecto forma parte de un portfolio orientado a:

* Analista de Datos
* Analytics Engineer
* Data Engineer (nivel inicial)

Demuestra capacidades en:

* diseño de pipelines de datos
* modelado analítico
* ingestión robusta
* separación de capas
* buenas prácticas de desarrollo

---

## 📌 Estado del proyecto

En desarrollo.
