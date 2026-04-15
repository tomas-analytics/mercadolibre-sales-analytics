# 🏗️ Arquitectura del sistema

## 🎯 Objetivo

Definir la arquitectura general del sistema de analytics de ventas de Mercado Libre, separando responsabilidades por capas para lograr escalabilidad, mantenibilidad y claridad.

---

## 🧱 Arquitectura en capas

El sistema está diseñado en 4 capas principales:

```text
Archivo Excel/CSV
        ↓
[ Ingesta - Python ]
        ↓
[ Data Warehouse - BigQuery ]
        ↓
[ Transformaciones - dbt ]
        ↓
[ Consumo - Streamlit ]
```

---

## 🔹 1. Capa de Ingesta (Python)

Responsabilidades:

* Lectura de archivos Excel/CSV
* Detección dinámica de header
* Normalización de nombres de columnas
* Mapeo a esquema interno
* Validación de datos
* Tipado de columnas
* Generación de metadata técnica
* Carga incremental a BigQuery

📁 Ubicación:

```text
etl/ingest/
```

---

## 🔹 2. Data Warehouse (BigQuery)

### Tablas principales

#### raw_ml_sales

* Contiene datos ya normalizados
* No depende del formato original del archivo

#### raw_ml_loads

* Auditoría de cargas
* Permite trazabilidad completa

---

## 🔹 3. Transformaciones (dbt)

Se divide en tres capas:

### STAGING

* Limpieza de datos
* Tipado final
* Normalización de strings

### CORE

* Modelo dimensional (star schema)
* Tablas:

  * dim_date
  * dim_product
  * dim_location
  * dim_status
  * dim_load
  * fct_sales

### MARTS

* Tablas listas para consumo analítico
* Optimizadas para consultas

---

## 🔹 4. Consumo (Streamlit)

Funcionalidades:

* Carga de archivos (upload)
* Visualización de KPIs
* Filtros interactivos
* Análisis exploratorio

📁 Ubicación:

```text
app/
```

---

## 🧠 Decisiones clave

### Separación de capas

Cada capa tiene una responsabilidad clara, evitando lógica duplicada.

---

### Modelo desacoplado de la fuente

Los datos se transforman a un esquema interno estándar en inglés.

---

### Ingesta robusta

El sistema no depende de:

* orden de columnas
* nombres exactos
* estructura fija

---

### Trazabilidad

Cada carga queda registrada con metadata técnica.

---

## 🚀 Beneficios

* Escalable
* Mantenible
* Reutilizable
* Preparado para múltiples fuentes
