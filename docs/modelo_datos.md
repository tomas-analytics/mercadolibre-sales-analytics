# 📊 Modelo de datos

## 🎯 Objetivo

Definir el esquema de datos interno del sistema, independiente del formato original de Mercado Libre.

---

## 🧱 Grano de datos

Cada fila representa:

👉 **1 venta**

---

## 🔑 Clave de negocio

`sale_id`

Origen:
"# de venta"

---

## 📌 Columnas obligatorias

```text
sale_id
sale_date
sale_status
quantity
total_amount_ars
```

Estas columnas son necesarias para que el registro sea válido.

---

## 📦 Columnas principales

### Información de la venta

* sale_id
* sale_date
* sale_status
* status_description

### Producto

* sku
* publication_id
* product_title
* variant

### Métricas

* quantity
* unit_price_ars
* product_revenue_ars
* total_amount_ars

### Costos

* shipping_cost_ars
* sale_fee_ars
* taxes_ars
* discounts_ars
* refund_amount_ars

### Cliente

* buyer_name
* buyer_document
* buyer_city
* buyer_state
* buyer_country

### Logística

* delivery_type
* shipped_at
* delivered_at
* carrier
* tracking_number

---

## 🧠 Modelo dimensional

El sistema utiliza un modelo tipo **star schema**.

### Tabla de hechos

#### fct_sales

Contiene métricas de ventas.

---

### Dimensiones

#### dim_date

Información temporal

#### dim_product

Información del producto

#### dim_location

Información geográfica

#### dim_status

Estado de la venta

#### dim_load

Metadata de carga

---

## 📊 Métricas principales

* Total de ventas
* Cantidad de ventas
* Unidades vendidas
* Ticket promedio
* Ventas por producto
* Ventas por ubicación
* Evolución temporal

---

## ⚠️ Consideraciones

* Existen columnas opcionales dependiendo de la versión del archivo
* La estructura económica puede variar
* El modelo es tolerante a columnas nuevas

---

## 🚀 Objetivo del modelo

* Simplificar análisis
* Mejorar performance
* Estandarizar métricas
* Facilitar escalabilidad
