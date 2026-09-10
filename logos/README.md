# Logos usados en la landing

Los SVG de esta carpeta son la fuente de verdad de los logos que aparecen
inlineados en `index.html`. Se inlinean para evitar peticiones extra y para que
hereden el tema de la página; estos archivos quedan versionados como origen.

| Archivo                  | Producto   | Origen                                              |
|--------------------------|------------|-----------------------------------------------------|
| `notebook-icon.svg`      | Notebook   | `NexFi/public/favicon.svg` (propio)                 |
| `motordesk-icon.svg`     | MotorDesk  | `motordesk/brand/logos/logo-icon.svg` (propio)      |
| `smartpos-icon.svg`      | SmartPOS   | `pos-cafeteria/public/smartpos.svg` (propio)        |
| `glpi-logo-color.svg`    | GLPI       | proyecto GLPI — ver nota de marca abajo             |

## Marcas propias

Notebook, MotorDesk y SmartPOS son productos de NexCore. Sus colores de marca:

- Notebook — obsidiana `#0A0A0F` con la "N" en blanco.
- MotorDesk — degradado naranja `#FB923C → #F97316 → #EA580C`, "M" en blanco.
- SmartPOS — ámbar `#d97706`, taza en blanco.

## Nota de marca sobre GLPI

`glpi-logo-color.svg` procede de los assets publicados por el proyecto GLPI
(`glpi-project/glpi`, `public/pics/logos/sources/GLPI_Logo-color.svg`). GLPI es
marca de Teclib', no de NexCore. Se usa aquí de forma descriptiva para
identificar el software que NexCore implementa y gestiona como servicio. No
implica respaldo, afiliación ni certificación por parte de Teclib'. Si Teclib'
solicita retirarlo, sustituir por la "G" estilizada propia.
