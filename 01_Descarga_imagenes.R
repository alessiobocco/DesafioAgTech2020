# -----------------------------------------------------------------------------#
# --- PASO 1. Inicializar entorno ----

# i. Borrar objetos existentes en el ambiente
rm(list = ls()); gc()

# ii. Configurar huso horario en UTC
Sys.setenv(TZ = "UTC")

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 2. Leer archivos YML de configuracion y parametros ----

# i. Leer YML de configuracion
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos.
  # Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuration.yml")
}

if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuracion de ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuracion ", archivo.config, "...\n"))
  config <- yaml::yaml.load_file(archivo.config)
}

# ii. YML de parametros para los controles de calidad
if (length(args) > 1) {
  archivo.params <- args[2]
} else {
  # No vino el archivo de parametros por linea de comandos.
  # Utilizo un archivo default
  archivo.params <- paste0(getwd(), "/parameters.yml")
}
if (! file.exists(archivo.params)) {
  stop(paste0("El archivo de parametros de ", archivo.params, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de parametros ", archivo.params, "...\n"))
  config$params <- yaml::yaml.load_file(archivo.params)
}

rm(archivo.config, archivo.params, args); gc()

# ------------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# --- Paso 3: Cargar e instalar paquetes necesarios ----
# ---------------------------------------------------------------------------- #


library(dplyr)
library(purrr)
library(raster)
library(sf)
library(tidyr)
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Cargar datos necesarios ----
# -----------------------------------------------------------------------------#
# Cargar datos necesarios

# Datos de train
train <- read.csv(glue::glue("{config$dir$input}/data_train.csv")) %>%
  dplyr::mutate(train = 1)
# Datos de test
train <- read.csv(glue::glue("{config$dir$input}/data_test.csv")) %>%
  dplyr::mutate(train = 0)
# Area de estudio
area_estudio <- sf::st_read(glue::glue("{config$dir$input}/Gral_Lopez/"),
  layer = 'Gral_Lopez') %>%
  sf::st_transform(., config$params$projections$gk) %>%
  sf::st_buffer(., dist = 10000) %>%
  sf::st_transform(., config$params$projections$latlon)

#sf::write_sf(area_estudio, glue::glue("{config$dir$input}/Gral_Lopez/area_estudio_buffer.shp"))
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- Paso 5. Descargar imagenes de 
# -----------------------------------------------------------------------------#
som <- function(x) {
  as.Date(format(x, "%Y-%m-01"))
}

eom <- function(x) {
  som(som(x) + 35) - 1
}

fechas.procesables <- data.frame(date =
  seq(from = as.Date(config$params$gge$date$begin_date),
  to = as.Date(config$params$gge$date$end_date),
  by = 'days')) %>%
  dplyr::mutate(month = lubridate::month(date),
    year = lubridate::year(date)) %>%
  dplyr::group_by(year, month) %>%
  dplyr::summarise() %>%
  dplyr::ungroup(.) %>%
  dplyr::mutate(date = lubridate::ymd(paste(year, month, 15L))) %>%
  dplyr::mutate(begin_month = som(date),
    end_month = eom(date)) 

## It is necessary just once
#rgee::ee_install()
# Inicializar entorno
#rgee::ee_Initialize(config$params$gge$usuario)
library(rgee)

rgee::ee_Initialize(email = "boccoalessio@gmail.com", drive = TRUE)

ee_check() # Check non-R dependencies
#ee_clean_credentials() # Remove credentials of a specific user
ee_clean_pyenv() # Remove reticulate system variables




# Definiendo un limite
#area_estudio_ee  <- ee$FeatureCollection('area_estudio')

area_estudio_ee  <- ee$FeatureCollection(config$params$gge$area_estudio)


# Seleccionando las bandas
bandas <- config$params$gge$bandas

# Filtrando los metadatos: usar Abril para mostrar la diferencia 
# en la selección de acuerdo al porcentaje de nubes (40, 10, 90).

purrr::map(
  .x = 1:nrow(fechas.procesables),
  .f = function(imagen) {
    
    fecha_i <- fechas.procesables[imagen,]
    
    imagenes_sentinel <- ee$ImageCollection(config$params$gge$coleccion)$
      select(bandas)$
      filterDate(as.character(fecha_i$begin_month), as.character(fecha_i$end_month))$
      filterMetadata('CLOUDY_PIXEL_PERCENTAGE','less_than', 10)$
      median()$
      clip(area_estudio_ee)
    
    # Define an area of interest.
    geometry <- ee$Geometry$Rectangle(
      coords = c(extent(area_estudio)[1], 
        extent(area_estudio)[3], 
        extent(area_estudio)[2], 
        extent(area_estudio)[4]),
      proj = "EPSG:4326",
      geodesic = FALSE
    )
    
    ## drive - Method 01
    img <- ee_as_raster(
      image = imagenes_sentinel,
      region = geometry,
      scale = 30
    )
  
    
    # Nombre del archivo raster
    if (config$params$gge$coleccion == 'COPERNICUS/S2') {
      coleccion = 'sentinel'
    } else {
      coleccion = 'landsat'
    }
    file.name <- glue::glue("{config$dir$input}/images/imagen_{coleccion}_{fecha_i$year}_{fecha_i$month}.tif")
    
    if (!fs::dir_exists(glue::glue("{config$dir$input}/images"))) {
      fs::dir_create(glue::glue("{config$dir$input}/images"))
    }
    
    # Guardar raster
    subset(img, bandas) %>%
      raster::writeRaster(., filename = file.name, overwrite = TRUE)
    
    # Borrar descargas temporales
    ee_clean_container(name = "rgee_backup", type = "drive")
    
  }
)

