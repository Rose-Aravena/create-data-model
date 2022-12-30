const knexConfig = require('./db.config');
const knex = require('knex')(knexConfig);
const parse = require('csv-parse').parse;
const fs = require('fs');

let allTables;

const getAllMaintableFiles = async () => {
    const file = await fs.promises.readdir('./tables/maintable');
    allTables = await knex.select('TABLE_NAME').from('ALL_TABLES').where('OWNER', 'PLAYGROUND');
    console.log(allTables)
    parsingCsvMainTable(file)
}

const parsingCsvMainTable = async (files) => {
    files.forEach(async file => {
        const data = await fs.promises.readFile(`./tables/maintable/${file}`, 'utf-8');
        parse(data, {delimiter: ',', columns:true, skip_empty_lines: true}, async (err, data) => {
            if (err) {
                console.error(err.message)
            }
            data.forEach( i => {
                delete i.IO;
                delete i.ÁMBITO;
                delete i.DESCRIPCIÓN;
                delete i.REQUERIDO;
                i['CAMPO BBDD'] = i['CAMPO BBDD']
                                    .replace(/ds_/, '')
                                    .replace(/ /g, '')
                                    .replace(/formulario/g, 'form')
                                    .replace(/numero/g, 'nro')
                                    .normalize('NFD')
                                    .replace(/[\u0300-\u036f]/g,"")
                                    .toUpperCase()
                                    .replace(/ /gi, '_')
                                    .replace(/(\([^\)]*\))/g, '');
                if(i['CAMPO BBDD'].length > 24 ){
                    i['CAMPO BBDD'] = i['CAMPO BBDD'].slice(0, 24-i['CAMPO BBDD'].length);
                }
            })

            console.log(data)
            let newstr = file.replace(/.csv/gi, '');
            newstr = newstr.normalize('NFD').replace(/[\u0300-\u036f]/g,"").toUpperCase().replace(/ /gi, '_')
            if(newstr.length > 24){
                newstr = newstr.slice(0, 24-newstr.length);
            };
            
            //await createMainTable(newstr, data);
            //await createForeignKeys(newstr, data)
        });
    })
};

const createMainTable = async (tableName, columns) => {
    try{
        await knex.schema.createTable(tableName, (table) => {
            table.integer(columns[0]['CAMPO BBDD']).primary();
            for (i=1; i < columns.length; i++){
                let type = columns[i]['TIPO'];
                let name = columns[i]['CAMPO BBDD'];
                const regex = /(\d+)/g;
                let varcharLength;
                if(type.match(regex)){
                    varcharLength = parseInt(type.match(regex))
                    type = 'Varchar';
                };
                switch (type) {
                    case 'Varchar':
                        table.string(name, [varcharLength])
                        break;
                    case 'Integer':
                        table.integer(name)
                        break;
                    case 'Date':
                        table.date(name)
                        break;
                    case 'Boolean':
                        table.boolean(name)
                        break;
                    case 'Geometry':
                        table.geometry(name)
                        break;
                }
            }
        })
        console.info('Table created');
    }catch(err) {
        console.error(err)
    }finally{
        knex.destroy()
    }
}

const createManualForeignKeys = async () => {
    try{
        await knex.schema.table('ILICITOS', table => {
            table.foreign('ACCION_MEDICION').references('IDENTIFICADOR').inTable('SUB_ACCION_MEDICION')
        })
        console.log('FK created')
    }catch(err){
        console.error(err.message)
    }finally{
        knex.destroy()
    }
}

const createForeignKeys = async (newstr, data) => {
    const columnsWithFK = data.filter( column => column['CAMPO BBDD'].includes('('));
    const nameColumnsWithFK = columnsWithFK.map( i => i['CAMPO BBDD']);
    let foreignColumn;
    nameColumnsWithFK.forEach( i => {
        const number = i.match(/(\d+)/g);
        const tableRegex = new RegExp("\\b"+number+"\\b");
        allTables.forEach( t => {
            if(t.TABLE_NAME.match(tableRegex)){
                knex.select().from(t.TABLE_NAME).then( data => {
                    foreignColumn = Object.keys(data[0])[0];
                    knex.schema.table(newstr, table => {
                        table.foreign(i).references(foreignColumn).inTable(t.TABLE_NAME)
                    }).then(console.log).catch(console.log)
                    console.log(typeof newstr, typeof i, typeof foreignColumn, typeof t.TABLE_NAME)
                })

            }
        })
    })
}


getAllMaintableFiles();
//createManualForeignKeys();


