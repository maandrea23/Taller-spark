# Taller local de Data Engineer

## Preparación

```bash
python3 extract_excel.py
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python etl.py
```

El Excel es la fuente disponible. `extract_excel.py` genera los CSV y JSONL que
el enunciado esperaba dentro de `datos/`. Los resultados quedan en `output/`.

## Limpieza y calidad

La limpieza se hace con PySpark; el Excel original no se modifica.

Se revisan estos problemas:

- duplicados por clave;
- claves huérfanas;
- cantidades inválidas;
- descuentos fuera de rango;
- precios inválidos;
- fechas mal formadas;
- estados con errores tipográficos;
- valores nulos.

Las filas rechazadas no se eliminan silenciosamente: se excluyen del resultado
válido y se justifican mediante los conteos y controles de calidad del proceso.

## Visualización de resultados

Para abrir los archivos Parquet de forma más cómoda en VS Code, se recomiendan
estas extensiones:

- **Parquet Lens**: permite abrir cada archivo `.parquet` como una tabla.
- **DuckDB**: permite consultar uno o varios archivos Parquet usando SQL.

Después de instalar las extensiones, abre un archivo dentro de
`output/monthly_sales/` con Parquet Lens. Para consultar todos los meses con
DuckDB, crea un archivo `consulta.sql` con:

```sql
SELECT *
FROM read_parquet('output/monthly_sales/**/*.parquet')
ORDER BY month, country, channel, category;
```

Para obtener el total mensual:

```sql
SELECT month, ROUND(SUM(net_sales), 2) AS total_ventas
FROM read_parquet('output/monthly_sales/**/*.parquet')
GROUP BY month
ORDER BY month;
```
