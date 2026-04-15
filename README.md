# Análisis de Productividad y Educación en Argentina
![Estado del Proyecto](https://img.shields.io/badge/Estado-En%20Refinamiento-yellowgreen?style=for-the-badge&logo=github)

> **Nota:** Este proyecto está siendo actualizado para mejorar la calidad visual de los reportes y la profundidad de las conclusiones analíticas.
>
> Análisis de Productividad y Educación en Argentina (Laboratorio de Datos I)
📌 Descripción del Proyecto

Este proyecto fue desarrollado para el Trabajo Práctico 1 de la materia Laboratorio de Datos I (UBA, 2do Cuatrimestre 2025). La investigación busca demostrar la estrecha relación entre el desarrollo productivo de Argentina y el nivel educativo de sus ciudadanos.

A través de la limpieza y el análisis de fuentes de datos oficiales, el estudio concluye que la productividad de una sociedad está inevitablemente atada a su educación.

🛠️ Tecnologías y Entorno

    IDE: Desarrollado íntegramente en Spyder.

    Lenguaje: Python 3.x.

    Librerías principales: * Pandas y NumPy para la limpieza y manipulación de datos.

    Matplotlib para la generación de visualizaciones estadísticas.

    SQL para consultas complejas sobre los datasets.

📂 Estructura del Repositorio

El proyecto se organiza de manera modular para separar la gestión de datos, la lógica de procesamiento y la documentación:

    data/: Contiene los conjuntos de datos en sus diferentes etapas.

        raw/: Archivos originales sin modificar.

        interim/: Datos en proceso de transformación.

        processed/: Datasets finales, limpios y normalizados bajo las tres formas normales.

    scripts/: Módulos de Python desarrollados íntegramente en Spyder:

        LimpiezaDatasets.py: Script para el tratamiento inicial y limpieza de las fuentes.

        MetricasDeCalidad.py: Implementación de la técnica GQM para medir eficiencia y calidad.

        TablasRelacional.py: Creación del modelo relacional y cumplimiento de formas normales.

        limpiezaConsultas.py: Ejecución de las 4 consultas SQL principales.

        Visualizaciones.py: Generación de gráficos estadísticos con Matplotlib.

    docs/: Documentación técnica y resultados.

        visualizaciones/:

            01_grafico_empleados_provincia.png 

            02_grafico_establecientos_vs_poblacion.png 

            03_grafico_boxplot.png

            04_grafico_relacion_empleados_establecimientos.png 

            05_grafico_proporcion_mujeres_por_actividad.png 
            
        Informe.pdf: Documento final con el análisis, metodología GQM y conclusiones.

        2025C2-TP01-Enunciado.pdf: Consignas oficiales del trabajo práctico.

🚀 Metodología y Procesamiento

El proyecto aplicó técnicas rigurosas de ingeniería de datos:

    Limpieza y Normalización: Se transformaron datasets originales que no cumplían con las Formas Normales (1FN y 2FN).

    Optimización de Rendimiento: Mediante la técnica GQM (Goal, Question, Metric), se logró reducir el tiempo de lectura del dataset educativo de 93.5 segundos a 0.14 segundos.

    Modelo Relacional: Diseño de un Diagrama Entidad-Relación (DER) para vincular establecimientos educativos, productivos y datos poblacionales por departamento.

    Análisis SQL: Ejecución de consultas para determinar la densidad de empleados y la representación femenina por rubro (CLAE).

📊 Hallazgos Principales

    Se observó una correlación directa entre la cantidad de habitantes de un nivel educativo y la disponibilidad de establecimientos de dicho nivel.

    Existe un marcado descenso poblacional entre el nivel primario y secundario, sugiriendo deserción escolar.

    El promedio de empleo femenino en las actividades productivas analizadas se sitúa en un 33.074%.

✒️ Integrantes (Grupo: "IMPORT_MILANESAS")

    Perez Sotelo Martina
    Dulio Joaquin 
    Risuleo Franco 

    
