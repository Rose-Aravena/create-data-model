const knexConfig = require('./db.config');
const knex = require('knex')(knexConfig);
const parse = require('csv-parse').parse;
const fs = require('fs');

let allTables;

const getAllMaintableFiles = async () => {
    const file = await fs.promises.readdir('./tables/maintable');
    allTables = await knex.select('TABLE_NAME').from('ALL_TABLES').where('OWNER', 'PLAYGROUND');
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
                i['campos'] = i['campos']
                                    .replace(/ds_/, '')
                                    .replace(/ /g, '')
                                    .replace(/formulario/g, 'form')
                                    .replace(/numero/g, 'nro')
                                    .normalize('NFD')
                                    .replace(/[\u0300-\u036f]/g,"")
                                    .toUpperCase()
                                    .replace(/ /gi, '_')
                                    .replace(/(\([^\)]*\))/g, '');
                if(i['campos'].length > 30 ){
                    i['campos'] = i['campos'].slice(0, 30-i['campos'].length);
                }
            })
            let newstr = file.replace(/.csv/gi, '');
            newstr = newstr.normalize('NFD').replace(/[\u0300-\u036f]/g,"").toUpperCase().replace(/ /gi, '_')
            if(newstr.length > 30){
                newstr = newstr.slice(0, 30-newstr.length);
            };
            for (let i=0; i < data.length; i++){
                let type = data[i]['Tipo'];
                data[i].params = null;
                if(type.startsWith('NUMBER')){
                    const num = type.match(/\((.*?)\)/);
                    if(!num){
                        data[i]['Tipo'] = 'INTEGER';
                    }else if(!num[1].includes(',')){
                        data[i]['Tipo'] = 'INTEGER';
                        data[i].params = num[1];
                    }
                    else{
                        data[i]['Tipo'] = 'FLOAT';
                        const params = num[1].split(',')
                        data[i].params = params;
                    }
                } else if(type.match(/(\d+)/g)){
                    data[i].params = type.match(/(\d+)/g)[1] ||  type.match(/(\d+)/g)[0]
                    data[i]['Tipo'] = 'VARCHAR';
                };
            } 
            //console.log(data)           
            //await createMainTable(newstr, data);
            await createForeignKeys(newstr, data)
        });
    })
};

const createMainTable = async (tableName, columns) => {
    try{
        await knex.schema.createTable(tableName, (table) => {
            table.integer(columns[0]['campos'], [columns[0]['params']]).primary();
            for (i=1; i < columns.length; i++){
                let type = columns[i]['Tipo'];
                let name = columns[i]['campos'];
                let params = columns[i]['params']
                switch (type) {
                    case 'VARCHAR':
                        table.string(name, [params]).unsigned()
                        break;
                    case 'INTEGER':
                        table.integer(name, [params]).unsigned()
                        break;
                    case 'FLOAT':
                        table.float(name, [params[0]], [params[1]]).unsigned()
                        break;
                    case 'DATE':
                        table.date(name).unsigned()
                        break;
                    case 'BOOLEAN':
                        table.boolean(name).unsigned()
                        break;
                    case 'GEOMETRY':
                        table.geometry(name).unsigned()
                        break;
                }
            }
        })
        console.info('Table created');
    }catch(err) {
        console.error(err)
    }
}

const createManualForeignKeys = async () => {
    try{
        await knex.schema.table('ILICITOS', table => {
            table.foreign('ACCION_MEDICION').references('IDENTIFICADOR').inTable('SUB_ACCION_MEDICION').onDelete('CASCADE')
        })
        console.log('FK created')
    }catch(err){
        console.error(err)
    }finally{
        knex.destroy()
    }
}

const createForeignKeys = async (tableName, columns) => {
    const allColumns = columns.map( col => col.campos);
    const allTablesNames = allTables.map( table => table.TABLE_NAME)
    allTablesNames.forEach( subTable => {
        allColumns.forEach( async col => {
            if(subTable.includes(col)){
                try{
                    const data = await knex.select().from(subTable);
                    const foreignColumn = Object.keys(data[0])[0];
                    await knex.schema.table(tableName, table => {
                        table.foreign(col).references(foreignColumn).inTable(subTable)
                    })
                    console.info('FK created')
                }catch(err){
                    console.error(1, err)
                }
            }
        })
    })
};

getAllMaintableFiles();
//createManualForeignKeys();





