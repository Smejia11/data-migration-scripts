# 🧩 Lead Updater Script

Este script conecta a una base de datos MongoDB, enriquece cada documento en la colección `leads` con información adicional proveniente de las colecciones `quotations` y `policyrequests`, y finalmente crea múltiples índices para optimizar las consultas futuras.

---

## 🚀 Requisitos

- Node.js >= 14
- Acceso a una instancia de MongoDB con las siguientes colecciones:
  - `leads`
  - `quotations`
  - `policyrequests`

---

## ⚙️ Variables de Entorno

Define las siguientes variables en un archivo `.env` o en tu entorno:

```env
MONGODB_URI=mongodb://usuario:contraseña@localhost:27017/nombre_basedatos
MONGODB_DB_NAME=nombre_basedatos
LIMITER=1000
