const { fields, table, whereClauses, joinType, joinTable, joinCondition, groupByFields, hasAggregateWithoutGroupBy } = parseQuery(query);
let data = await readCSV(`${table}.csv`);

// Perform INNER JOIN if specified
if (joinTable && joinCondition) {
    const joinData = await readCSV(`${joinTable}.csv`);
    data = data.flatMap(mainRow => {
        return joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });

    
}


// Apply WHERE clause filtering after JOIN (or on the original data if no join)
let filteredData = whereClauses.length > 0
    ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
    : data;
    
    filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            // Assuming 'field' is just the column name without table prefix
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });

    function performInnerJoin(/* parameters */) {
        // Logic for INNER JOIN
        // ...
        return data.flatMap(mainRow => {
            return joinData
                .filter(joinRow => {
                    const mainValue = mainRow[joinCondition.left.split('.')[1]];
                    const joinValue = joinRow[joinCondition.right.split('.')[1]];
                    return mainValue === joinValue;
                })
                .map(joinRow => {
                    return fields.reduce((acc, field) => {
                        const [tableName, fieldName] = field.split('.');
                        acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                        return acc;
                    }, {});
                });
        });
    }
    
    function performLeftJoin(/* parameters */) {
        // Logic for LEFT JOIN
        // ...
        return data.flatMap(mainRow => {
            const matchingJoinRows = joinData.filter(joinRow => {
                const mainValue = getValueFromRow(mainRow, joinCondition.left);
                const joinValue = getValueFromRow(joinRow, joinCondition.right);
                return mainValue === joinValue;
            });
    
            if (matchingJoinRows.length === 0) {
                return [createResultRow(mainRow, null, fields, table, true)];
            }
    
            return matchingJoinRows.map(joinRow => createResultRow(mainRow, joinRow, fields, table, true));
        });
    }
    
    function performRightJoin(/* parameters */) {
        // Logic for RIGHT JOIN
        // ...
            // Cache the structure of a main table row (keys only)
    const mainTableRowStructure = data.length > 0 ? Object.keys(data[0]).reduce((acc, key) => {
        acc[key] = null; // Set all values to null initially
        return acc;
    }, {}) : {};

    return joinData.map(joinRow => {
        const mainRowMatch = data.find(mainRow => {
            const mainValue = getValueFromRow(mainRow, joinCondition.left);
            const joinValue = getValueFromRow(joinRow, joinCondition.right);
            return mainValue === joinValue;
        });

        // Use the cached structure if no match is found
        const mainRowToUse = mainRowMatch || mainTableRowStructure;

        // Include all necessary fields from the 'student' table
        return createResultRow(mainRowToUse, joinRow, fields, table, true);
    });
    }
    
    async function executeSELECTQuery(query) {
        const { fields, table, whereClauses, joinType, joinTable, joinCondition } = parseQuery(query);
        let data = await readCSV(`${table}.csv`);
    
        // Logic for applying JOINs
        if (joinTable && joinCondition) {
            const joinData = await readCSV(`${joinTable}.csv`);
            switch (joinType.toUpperCase()) {
                case 'INNER':
                    data = performInnerJoin(data, joinData, joinCondition, fields, table);
                    break;
                case 'LEFT':
                    data = performLeftJoin(data, joinData, joinCondition, fields, table);
                    break;
                case 'RIGHT':
                    data = performRightJoin(data, joinData, joinCondition, fields, table);
                    break;
                // Handle default case or unsupported JOIN types
            }
        }
    
        // ...existing code for WHERE clause and field selection...
    }
    
    module.exports = executeSELECTQuery;
    