const knexConfig = require("./db.config");
const knex = require("knex")(knexConfig);

const createView = (
  nombre_vista,
  nombre_tabla_ppal,
  nombre_subtabla,
  campo_pk
) => {
  knex.schema
    .createView("nombre_view", function (view) {
      view.as(
        knex("nombre_tabla_principal")
          .join(
            "nombre_subtabla",
            "nombre_tabla_principal.nombre_campo_primary_key",
            "=",
            "nombre_subtabla.nombre_campo_foreign_key"
          )

          .select("False_Table.Id", "SUB_ACCION_CAMBIO.TIPO_ACCION")
      );
    })
    .then(() => {
      console.log("Estamos ok");
    });
};

// ----- Primer ejemplo funcional de creacion de vistas -----

// knex.schema
//   .createView("VW_TEST_ACCION_CAMBIO3", function (view) {
//     view.as(
//       knex("False_Table")
//         .join(
//           "SUB_ACCION_CAMBIO",
//           "False_Table.Foreign",
//           "=",
//           "SUB_ACCION_CAMBIO.IDENTIFICADOR"
//         )
//         .select("False_Table.Id", "SUB_ACCION_CAMBIO.TIPO_ACCION")
//     );
//   })
//   .then(() => {
//     console.log("Estamos ok");
//   });

// ---------- Función Real ----------

// const createView = (
//   nombre_vista,
//   nombre_tabla_ppal,
//   nombre_subtabla,
//   campo_pk
// ) => {
//   knex.schema
//     .createView("nombre_view", function (view) {
//       view.as(
//         knex("nombre_tabla_principal")
//           .join(
//             "nombre_subtabla",
//             "nombre_tabla_principal.nombre_campo_primary_key",
//             "=",
//             "nombre_subtabla.nombre_campo_foreign_key"
//           )

//           .select("False_Table.Id", "SUB_ACCION_CAMBIO.TIPO_ACCION")
//       );
//     })
//     .then(() => {
//       console.log("Estamos ok");
//     });
// };

// const arrayJoins = [
//   {
//     nombreTablaPrincipal: "Dummy",
//     nombreSubtabla: "Dummy",
//     campoForeignKey: "Dummy",
//   },
// ];

// knex.schema.createTable("DUMMY_FOREIGNS", function (table) {
//   table.string("ID");
//   table.string("SUB_RELACION");
//   table.integer("SUB");
// });
