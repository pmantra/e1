/**
 * Constants representing common attribute names that might be used for differentiating
 * the sub-populations
 *
 * @type {string[]}
 */
const ATTRIBUTE_NAME_SUGGESTIONS = [
    "custom_attributes.XrefIdPartitionNumber",
    "record.clientId",
    "record.clientName",
    "record.country",
    "record.customerId",
    "record.customerName",
    "record.insurance_plan",
    "record.lob",
    "record.plan_carrier",
    "record.plan_name",
    "work_state",
]

/**
 * Constants representing special cases to handle for attribute processing
 *
 * @type {{IS_NULL: string, DEFAULT_CASE: string}}
 */
const SPECIAL_CASE_ATTRIBUTES = {
    "DEFAULT_CASE": "ATTRIBUTE_DEFAULT_CASE",
    "IS_NULL": "ATTRIBUTE_IS_NULL"
};

/**
 * The maximum number of recursive calls to make to prevent infinite recursion
 *
 * @type {number}
 */
const MAX_RECURSION_DEPTH = 42;

//#region Initial Setup / Data Creation

/**
 * Overrides the "Enter" key handler to prevent unintentionally submitting the form
 */
function overrideDateTimeEnterHandlers() {
    addEnterEventTranslationHandler(document.getElementById("activated_at"));
    addEnterEventTranslationHandler(document.getElementById("deactivated_at"));
}

/**
 * Creates and returns the attribute criteria structure that holds the attribute information
 *
 * @param attrName - Attribute name
 * @param allowNull - Determines whether to allow NULL values. Default is false.
 * @param allowDefaultCase - Determines whether to enable a default handler. Default is false.
 * @returns {{values: Set<any>, name, allowDefaultCase: boolean, allowNull: boolean}}
 *
 * @example:
 *  Input:
 *      attrName = "work_state"
 *  Output:
 *  {
 *      "name": "work_state",
 *      "allowNull": false,
 *      "allowDefaultCase": false,
 *      "values": new Set()
 *  }
 */
function createAttributeCriteria(attrName, allowNull = false, allowDefaultCase = false) {
    return {
        "name": attrName,
        "allowNull": allowNull,
        "allowDefaultCase": allowDefaultCase,
        "values": new Set()
    };
}

/**
 * Creates and returns a criteria map, as extracted from the JSON object that is passed
 * in from the DB to the backend to Jinja to Javascript. Converting the criteria map to
 * Map (instead of using the structure as a dictionary) allows usage of null as a key. This
 * function is used to create the map at load time from what was stored in the DB.
 *
 * @param criteriaMapObj - The criteria map as a JSON object
 * @returns {Map<any, any>} - The criteria map
 *
 * @example:
 *  Input:
 *      {
 *          "a": {
 *              "1": {
 *                  "alpha": 101,
 *                  "omega": 199
 *              },
 *              "2": {
 *                  "alpha": 201,
 *                  "omega": 299
 *              }
 *          }
 *      }
 *  Output:
 *      Nested Map object representation of the same data
 */
function createCriteriaMapFromObj(criteriaMapObj) {
    let newCriteriaMap = new Map();
    Object.entries(criteriaMapObj).forEach(
        ([key, value]) => {
            if (value instanceof Object) {
                newCriteriaMap.set(key, createCriteriaMapFromObj(value));
            } else {
                newCriteriaMap.set(key, value);
            }
        }
    );
    return newCriteriaMap;
}

/**
 * Creates and return a criteria map, as extracted from all the attributes defined
 * in the attribute criteria list. This will ensure that the map covers all possible
 * combinations of attribute values. This function is used to create the map as
 * the attribute criteria list changes.
 *
 * @param attributeCriteriaList - A list of attributeCriteria structures as defined
 *  in createAttributeCriteria
 * @returns {Map<any, any>} - The criteria map
 *
 * @example:
 *  Input:
 *  [
 *     {
 *          "name": "letters",
 *          "allowNull": false,
 *          "allowDefaultValues": false,
 *          "values": Set({"a", "b"})
 *      },
 *      {
 *          "name": "numbers",
 *          "allowNull": false,
 *          "allowDefaultValues": true,
 *          "values": Set({"1", "2"})
 *      },
 *      {
 *          "name": "greek",
 *          "allowNull": false,
 *          "allowDefaultValues": false,
 *          "values": Set({"alpha", "omega"})
 *      }
 *  ]
 * Output:
 *  {
 *      "a": {
 *          "1": {
 *              "alpha": null,
 *              "omega": null
 *          },
 *          "2": {
 *              "alpha": null,
 *              "omega": null
 *          },
 *          "ATTRIBUTE_DEFAULT_CASE": {
 *             "alpha": null,
 *              "omega": null
 *          }
 *      },
 *      "b": {
 *         "1": {
 *              "alpha": null,
 *              "omega": null
 *          },
 *          "2": {
 *              "alpha": null,
 *              "omega": null
 *          },
 *          "ATTRIBUTE_DEFAULT_CASE": {
 *              "alpha": null,
 *              "omega": null
 *          }
 *      }
 *  }
 */
function createCriteriaMapFromAttributeCriteriaList(attributeCriteriaList) {
    let newCriteriaMap = new Map();
    attributeCriteriaList.toReversed().forEach(
        (attribute) => {
            let updatedCriteriaMap = new Map();
            if (newCriteriaMap.size === 0) {
                attribute.values.forEach(
                    (attrValue) => {
                        updatedCriteriaMap.set(attrValue, null);
                    }
                )
                // Special cases
                if (attribute.allowNull) {
                    updatedCriteriaMap.set(SPECIAL_CASE_ATTRIBUTES.IS_NULL, null);
                }
                if (attribute.allowDefaultCase) {
                    updatedCriteriaMap.set(SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE, null);
                }
            } else {
                attribute.values.forEach(
                    (attrValue) => {
                        updatedCriteriaMap.set(attrValue, structuredClone(newCriteriaMap));
                    }
                );
                if (attribute.allowNull) {
                    updatedCriteriaMap.set(SPECIAL_CASE_ATTRIBUTES.IS_NULL, structuredClone(newCriteriaMap));
                }
                if (attribute.allowDefaultCase) {
                    updatedCriteriaMap.set(SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE, structuredClone(newCriteriaMap));
                }
            }
            if (updatedCriteriaMap.size === 0) {
                if (newCriteriaMap.size > 0) {
                    updatedCriteriaMap.set(null, newCriteriaMap);
                } else {
                    updatedCriteriaMap.set(null, null);
                }
            }
            newCriteriaMap = updatedCriteriaMap;
        }
    );
    return newCriteriaMap;
}

/**
 * Creates and returns a list of attribute criteria structures based on the provided attribute
 * name list and criteria map.
 *
 * @param attributeNameList - A list of attribute names
 * @param criteriaMap - The map of the attribute values to the sub-population IDs
 * @returns {*|*[]} - A list of attribute criteria structures
 *
 * @example
 *  Input:
 *      attributeNameList = ["letters", "numbers", "greek"]
 *      criteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": null,
 *                     "omega": null
 *                 },
 *                 "2": {
 *                     "alpha": null,
 *                     "omega": null
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": null,
 *                     "omega": null
 *                 }
 *             }
 *         }
 *  Output:
 *      [
 *          {
 *              "name": "letters",
 *              "allowNull": false,
 *              "allowDefaultValues": false,
 *              "values": Set({ "a" })
 *          },
 *          {
 *              "name": "numbers",
 *              "allowNull": false,
 *              "allowDefaultValues": true,
 *              "values": Set({ "1", "2"})
 *          },
 *          {
 *              "name": "greek",
 *              "allowNull": false,
 *              "allowDefaultValues": false,
 *              "values": Set({ "alpha", "omega"})
 *          }
 *      ]
 */
function createAttributeCriteriaList(attributeNameList, criteriaMap) {
    let attributeCriteriaList = [];

    // Create the initial structure
    attributeNameList.forEach(
        (attributeName) => attributeCriteriaList.push(createAttributeCriteria(attributeName))
    );

    // Populate the structure
    attributeCriteriaList = extractAttributeValues(attributeCriteriaList, criteriaMap);

    return attributeCriteriaList;
}

/**
 * Takes a list of attribute criteria structures and populates the value information
 * using the criteria map provided, and then returns that list.
 *
 * @param attributeCriteriaList - The list of attribute criteria without values
 * @param criteriaMap - The criteria map of the population
 * @returns {*} - The list of attribute criteria with attribute values populated
 *
 * @example
 *  Input:
 *      attributeCriteriaList = [
 *          {
 *              "name": "letters",
 *              "allowNull": false,
 *              "allowDefaultValues": false,
 *              "values": Set()
 *          },
 *          {
 *              "name": "numbers",
 *              "allowNull": false,
 *              "allowDefaultValues": true,
 *              "values": Set()
 *          },
 *          {
 *              "name": "numbers",
 *              "allowNull": false,
 *              "allowDefaultValues": true,
 *              "values": Set()
 *          }
 *      ]
 *      criteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 101,
 *                     "omega": 199
 *                 },
 *                 "2": {
 *                     "alpha": 201,
 *                     "omega": 299
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": 1,
 *                     "omega": 99
 *                 }
 *             }
 *         }
 *  Updates attributeCriteriaList to be:
 *      attributeCriteriaList = [
 *          {
 *              "name": "letters",
 *              "allowNull": false,
 *              "allowDefaultValues": false,
 *              "values": Set({ "a" })
 *          },
 *          {
 *              "name": "numbers",
 *              "allowNull": false,
 *              "allowDefaultValues": true,
 *              "values": Set({ "1", "2"})
 *          },
 *          {
 *              "name": "numbers",
 *              "allowNull": false,
 *              "allowDefaultValues": false,
 *              "values": Set({ "alpha", "omega"})
 *          }
 *      ]
 */
function extractAttributeValues(attributeCriteriaList, criteriaMap) {
    criteriaMap.forEach(
        (attrMap, attrValue) => {
            // If the value is another map, parse it for the next attribute's values
            if (attrMap instanceof Map) {
                extractAttributeValues(attributeCriteriaList.slice(1), attrMap);
            }

            // Handle special values by setting the relevant bool, otherwise just
            // store the value
            if (attrValue === SPECIAL_CASE_ATTRIBUTES.IS_NULL) {
                attributeCriteriaList[0].allowNull = true;
            } else if (attrValue === SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE) {
                attributeCriteriaList[0].allowDefaultCase = true;
            } else {
                attributeCriteriaList[0].values.add(attrValue);
            }
        }
    )

    return attributeCriteriaList;
}

//#endregion

//#region Utility Functions

/**
 * Escapes a string so that it can be properly used in HTML
 *
 * @param s
 * @returns {*}
 *
 * @example
 *  Input:
 *      s = "Can't & Won't"
 *  Output:
 *      "Can&apos;t &amp; Won&apos;t"
 */
function escapeHTML(s) {
    const lookup = {
        "&": "&amp;",
        "\"": "&quot;",
        "'": "&apos;",
        "<": "&lt;",
        ">": "&gt;"
    };
    return s.replace( /[&"'<>]/g, c => lookup[c] );
}

//#endregion

//#region UI Event Handlers

/**
 * Handler for sub-population select element change event
 */
function handleSubPopSelectOnChange() {
    updateSubPopSelect(this);
    updateCriteriaMappingValues(this);
}

/**
 * Handler for "allow null" checkbox change event
 */
function handleAllowNullOnChange(attributeIndex) {
    updateAttributeAllowNull(attributeIndex);
}

/**
 * Handler for "allow default" checkbox change event
 */
function handleAllowDefaultOnChange(attributeIndex) {
    updateAttributeAllowDefault(attributeIndex);
}

/**
 * Handler for sub-population name text input change event
 *
 * @param subPopNameElement - A reference to the text input element since it holds the
 * current and previous values
 * @param isNewSubPop - A boolean to indicate if this is for new sub-population since
 * they need to be handled differently
 */
function handleSubPopulationNameOnChange(subPopNameElement, isNewSubPop = false) {
    if (areValidSubPopulationNames()) {
        if (isNewSubPop) {
            replaceCriteriaMappingValues(
                globalCriteriaMap,
                `<${subPopNameElement.dataset.prevValue} (New)>`,
                `<${subPopNameElement.value} (New)>`
            );
        }
        updateMappingTable();
        if (isNewSubPop) {
            updateSubPopLookupMapJSON();
        }
        // Save current value for future comparison
        subPopNameElement.dataset.prevValue = subPopNameElement.value;
    } else {
        alert("There are duplicate sub-population named. Please ensure that sub-population names are unique within a population.");
    }
}

/**
 * Handler for the button to add a new attribute name
 */
function handleAddAttributeNameOnClick() {
    addAttribute(document.getElementById("new_attribute_name"));
}

/**
 * Handler for the button to delete the attribute name indicated by the attribute
 * name select element
 */
function handleDelAttributeNameOnClick(attributeIndex) {
    deleteAttribute(attributeIndex);
}

/**
 * Handler for the button to add a new attribute value
 */
function handleAddAttributeValueOnClick(attributeIndex) {
    addAttributeValue(
        attributeIndex,
        document.getElementById(`new_attribute_value_${attributeIndex}`)
    );
}

/**
 * Handler for the button to delete the attribute value indicated by the attribute
 * value select element
 */
function handleDelAttributeValueOnClick(attributeIndex, attributeValue) {
    deleteAttributeValue(
        attributeIndex,
        attributeValue
    );
}

/**
 * Handler for the buttons that indicate that the user wants to delete/remove a
 * sub-population. There is a distinction between new and existing sub-pops
 * because an existing sub-pop needs to be deleted from the database, whereas
 * a new sub-pop only needs to be deleted from memory.
 *
 * @param removalElement - The div element containing the information of the
 * sub-population to be removed
 * @param isNew - A boolean to indicate if this is for new sub-population since
 * they need to be handled differently
 */
function handleSubPopulationRemoveOnClick(removalElement, isNew = false) {
    if ((isNew) || (removalElement.checked)) {
        if (confirm("Are you sure you want to delete this sub-population? Any mappings that use this sub-population will be unassigned.")) {
            let subPopulationDivId;
            if (!isNew) {
                // Get the sub-population inline-field ID by removing "del-"
                subPopulationDivId = removalElement.id.slice(4);
            } else {
                // Get the sub-population inline-field ID by removing "-remove"
                subPopulationDivId = removalElement.id.slice(0, -7);
            }
            const subPopulationDiv = document.getElementById(subPopulationDivId);
            if (isNew) {
                subPopulationDiv.remove();
            }
            let subPopIdentifier = null;
            if (!isNew) {
                const subPopIdElements = subPopulationDiv.getElementsByClassName("sub_population_id");
                if (subPopIdElements.length === 1) {
                    subPopIdentifier = subPopIdElements[0].value;
                }
            } else {
                const subPopNameElements = subPopulationDiv.getElementsByClassName("sub_population_name");
                if (subPopNameElements.length === 1) {
                    subPopIdentifier = `<${subPopNameElements[0].value} (New)>`;
                }
            }
            if (subPopIdentifier !== null) {
                replaceCriteriaMappingValues(
                    globalCriteriaMap,
                    subPopIdentifier,
                    null);
            }
            updateMappingTable();
            updateSubPopLookupMapJSON();
        } else if (!isNew) {
            removalElement.checked = false;
        }
    }
}

/**
 * Handler for the form submit event
 *
 * @returns {boolean} - The boolean return value indicates whether or not to proceed with the
 * form submission
 */
function handleEditFormOnSubmit() {
    // Data validation can be done here, returning false if invalid
    // Things to consider:
    // - Unused sub-pops
    // - Unmapped criteria
    return true;
}

//#endregion

//#region Input Validation

/**
 * Returns whether or not all sub-population names are valid. Currently, it only checks for
 * duplicate names, however, this can be expanded in the future to confirm other naming
 * rules TBD.
 *
 * @returns {boolean}
 */
function areValidSubPopulationNames() {
    // Go through all population names and highlight conflicts
    let subPopNames = new Set();
    const subPopContainers = document.querySelectorAll(".sub_population_container .sub_population_name");
    for (let i = 0; i < subPopContainers.length; i++) {
        const subPopName = subPopContainers[i].value;
        if (subPopNames.has(subPopName)) {
            return false;
        }
        subPopNames.add(subPopName);
    }

    return true;
}

//#endregion

//#region UI Manipulation

/**
 * This function is called when attribute criteria have been added or deleted. It, in turn,
 * calls other functions to update various sections of the UI for the change in attributes.
 */
function propagateAttributesChange() {
    // Update the sub_pop_lookup_keys_csv
    updateSubPopLookupKeysCSV();
    // Update the attributes UI
    updateAttributeUI();
    // Update the criteria mapping
    updateCriteriaMapping();
}

/**
 * Propagates attribute value changes to the UI by calling the functions that would update
 * the attribute values.
 */
function propagateAttributeValuesChange() {
    // Update the Attribute UI
    updateAttributeUI();
    // Update the criteria mapping
    updateCriteriaMapping();
}

/**
 * Updates the sub_pop_lookup_keys_csv, which is important as this is the value that will
 * be saved during the update.
 */
function updateSubPopLookupKeysCSV() {
    let subPopLookupKeysCSV = document.getElementById("sub_pop_lookup_keys_csv");
    subPopLookupKeysCSV.value = globalAttributeCriteriaList.map((attr) => attr.name).join(",");
}

/**
 * Updates the attributes table element using the data stored in globalAttributeCriteriaList
 */
function updateAttributesTable() {
    let attributesTable = document.getElementById("attributes_table");
    attributesTable.innerText = "";
    // Table body
    let attributesTableBody = document.createElement("tbody");
    let tempRow = document.createElement("tr");
    tempRow.innerHTML = getAttributeRowNewAttributeHtmlString();
    attributesTableBody.appendChild(tempRow);

    for (let attributeIndex = 0; attributeIndex < globalAttributeCriteriaList.length; attributeIndex++) {
        let attribute = globalAttributeCriteriaList[attributeIndex];

        tempRow = document.createElement("tr");
        tempRow.innerHTML = getAttributeRowSettingsHtmlString(attribute, attributeIndex);
        attributesTableBody.appendChild(tempRow);

        tempRow = document.createElement("tr");
        tempRow.innerHTML = getAttributeRowNewValueHtmlString(attributeIndex);
        attributesTableBody.appendChild(tempRow);

        tempRow = document.createElement("tr");
        tempRow.innerHTML = getAttributeRowExistingValuesHtmlString(attribute, attributeIndex);
        attributesTableBody.appendChild(tempRow);
    }
    attributesTable.appendChild(attributesTableBody);

    // Attach the key handlers
    addEnterEventTranslationHandler(document.getElementById("new_attribute_name"));
    const newAttributeValueInputs = document.getElementsByClassName("new_attribute_value");
    for (let i = 0; i < newAttributeValueInputs.length; i++) {
        addEnterEventTranslationHandler(newAttributeValueInputs.item(i));
    }
}

/**
 * Creates an event handler to reinterpret "Enter" keydown events as mouse clicks on a target element, specified
 * by adding a targetElementId as an attribute of the source element. If no target element is specified, the
 * handler will still prevent the "Enter" event from being processed by the default browser handler, which would
 * otherwise submit the form
 *
 * @param sourceElement - The element that is to have its "Enter" key handler overridden
 */
function addEnterEventTranslationHandler(sourceElement) {
    if (sourceElement) {
        sourceElement.addEventListener(
            "keydown",
            (event) => {
                if (event.code === "Enter") {
                    if (sourceElement.hasAttribute("targetElementId")) {
                        const targetElement = document.getElementById(sourceElement.getAttribute("targetElementId"));
                        if (targetElement) {
                            targetElement.dispatchEvent(new MouseEvent("click"));
                        }
                    }
                    event.preventDefault();
                }
            }
        );
    }
}

/**
 * Gets the HTML string for the row of the AttributeTable which has the area to create new attributes
 *
 * @returns {string}
 */
function getAttributeRowNewAttributeHtmlString() {
    return `<td colspan="4">` +
        `<label class="control-label">Attributes</label>` +
        `<div class="pull-right">` +
        (globalWasActivated ? `` : `<input type="text" id="new_attribute_name" list="attribute_name_suggestions" size="38" targetElementId="add_new_attribute_name_button">&nbsp;<input type="button" id="add_new_attribute_name_button" onclick="handleAddAttributeNameOnClick()" value="Add" />`) +
        `</div>` +
        `</td>`;
}

/**
 * Gets the HTML for the row of the AttributeTable which has the attribute's name, its settings, and the delete button
 *
 * @returns {string}
 */
function getAttributeRowSettingsHtmlString(attribute, attributeIndex) {
    return `<td rowspan="3">` +
        `<label class="control-label">${escapeHTML(attribute.name)}</label>` +
        `</td>` +
        `<td>` +
        `<input type="checkbox"${(attribute.allowNull ? ` checked` : ``)} id="attribute_allow_null_${attributeIndex}"${(globalWasActivated ? ` disabled` : ``)} onchange="handleAllowNullOnChange(${attributeIndex})">&nbsp;Allow NULL values` +
        `</td>` +
        `<td>` +
        `<input type="checkbox"${(attribute.allowDefaultCase ? ` checked` : ``)} id="attribute_allow_default_${attributeIndex}"${(globalWasActivated ? ` disabled` : ``)} onchange="handleAllowDefaultOnChange(${attributeIndex})">&nbsp;Allow default handler` +
        `</td>` +
        `<td>` +
        (globalWasActivated ? `` : `<div class="pull-right" onclick="handleDelAttributeNameOnClick(${attributeIndex})"><span class="fa fa-times glyphicon glyphicon-remove"></span>`) +
        `</td>`;
}

/**
 * Gets the HTML for the row of the AttributeTable which has the area to create new attribute values
 *
 * @returns {string}
 */

function getAttributeRowNewValueHtmlString(attributeIndex) {
    return `<td colspan="3">` +
        `<label class="control-label">Values</label>` +
        (globalWasActivated ? `` : `<div class="pull-right"><input type="text" id="new_attribute_value_${attributeIndex}" class="new_attribute_value" size="38" targetElementId="add_attribute_value_button_${attributeIndex}">&nbsp;<input type="button" id="add_attribute_value_button_${attributeIndex}" onclick="handleAddAttributeValueOnClick(${attributeIndex})" value="Add" /></div>`) +
        `</td>`;
}

/**
 * Gets the HTML for the row of the AttributeTable which has the attribute's values. This row is more complex
 * as its cell contains an inner table to hold the attribute values.
 *
 * @returns {string}
 */
function getAttributeRowExistingValuesHtmlString(attribute, attributeIndex) {
    let attributeRowExistingValuesHtmlString = `<td colspan="3">` +
        `<table class="table table-striped table-bordered table-hover">`;
    attribute.values.forEach(
        (attributeValue) => {
            attributeRowExistingValuesHtmlString +=
                `<tr><td>${escapeHTML(attributeValue)}` +
                (globalWasActivated ? `` : (`<div class="pull-right" onclick="handleDelAttributeValueOnClick(${attributeIndex}, '${escapeHTML(attributeValue.replaceAll("'", "\\'"))}')"><span class="fa fa-times glyphicon glyphicon-remove"></span></div>`)) +
                `</td></tr>`;
        }
    )
    if (attribute.allowNull) {
        attributeRowExistingValuesHtmlString +=
            `<tr><td>${SPECIAL_CASE_ATTRIBUTES.IS_NULL}` +
            (globalWasActivated ? `` : (`<div class="pull-right" onclick="handleDelAttributeValueOnClick(${attributeIndex}, '${SPECIAL_CASE_ATTRIBUTES.IS_NULL}')"><span class="fa fa-times glyphicon glyphicon-remove"></span></div>`)) +
            `</td></tr>`;
    }
    if (attribute.allowDefaultCase) {
        attributeRowExistingValuesHtmlString +=
            `<tr><td>${SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE}` +
            (globalWasActivated ? `` : (`<div class="pull-right" onclick="handleDelAttributeValueOnClick(${attributeIndex}, '${SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE}')"><span class="fa fa-times glyphicon glyphicon-remove"></span></div>`)) +
            `</td></tr>`;
    }
    attributeRowExistingValuesHtmlString += `</table></td>`;
    return attributeRowExistingValuesHtmlString;
}

/**
 * Updates the sub_pop_lookup_map_json, which is important as this is the value that will
 * be saved during the update.
 */
function updateSubPopLookupMapJSON() {
    let subPopLookupMapJSON = document.getElementById("sub_pop_lookup_map_json");
    subPopLookupMapJSON.value = JSON.stringify(Object.fromEntries(globalCriteriaMap), (key, value) => (value instanceof Map ? Object.fromEntries(value) : value));
}

/**
 * Updates the list of suggested attribute names, removing those that have already been used
 */
function updateAttributeNameSuggestions() {
    let attributeNameSuggestions = document.getElementById("attribute_name_suggestions");
    attributeNameSuggestions.innerText = "";
    let attributeNameSet = new Set();
    globalAttributeCriteriaList.forEach(
        (attribute) => { attributeNameSet.add(attribute.name); }
    )
    ATTRIBUTE_NAME_SUGGESTIONS.forEach(
        (suggestion) => {
            if (!(attributeNameSet.has(suggestion))) {
                let suggestionOption = document.createElement("option");
                suggestionOption.value = suggestion;
                suggestionOption.text = suggestion;
                attributeNameSuggestions.appendChild(suggestionOption);
            }
        }
    )
}

/**
 * Calls other functions to update the UI to match with the data of the attribute shown in the
 * selection element.
 */
function updateAttributeUI() {
    updateAttributeNameSuggestions();
    updateAttributesTable();
}

/**
 * Creates the table head (<thead>) that is to be used for the criteria mapping
 * table. It includes column headers based on the globalAttributeCriteriaList.
 *
 * @returns {HTMLTableSectionElement}
 */
function createMappingHead() {
    let mappingHead = document.createElement("thead");
    let innerHTML = `<tr>`;
    globalAttributeCriteriaList.forEach(
        (attribute) => {
            innerHTML += `<th class="column-header">${attribute.name}</th>`;
        }
    )
    innerHTML += `<th class="column-header">Sub-Population</th></tr>`;
    mappingHead.innerHTML = innerHTML;
    return mappingHead;
}

/**
 * Extracts the sub-population ID and name from the ID and name elements within the
 * provided sub-population element.
 *
 * @param subPopContainer - A reference to the sub-population element that contains
 * the the elements that contain the name and ID information
 * @returns {string[]} - An array containing the sub-population information, with the
 * first item containing the ID and the second item containing the name.
 */
function extractSubPopInfoFromContainer(subPopContainer) {
    let subPopId = "", subPopName = "";
    let tempElementArray = subPopContainer.getElementsByClassName("sub_population_id");
    if ((tempElementArray != null) && (tempElementArray.length > 0)) {
        subPopId = tempElementArray[0].value;
    }
    tempElementArray = subPopContainer.getElementsByClassName("sub_population_name");
    if ((tempElementArray != null) && (tempElementArray.length > 0)) {
        subPopName = tempElementArray[0].value;
    }
    return [subPopId, subPopName];
}

/**
 * Creates the sub-population select element, as well as option elements representing
 * the available sub-populations. This element will be cloned as part of creating the
 * criteria table body rows.
 *
 * @returns {HTMLSelectElement}
 */
function createMappingSubPopOptions() {
    let subPopSelect = document.createElement("select");
    subPopSelect.className = "form-control sub-pop-select";

    let subPopOption = document.createElement("option");
    subPopOption.text = "Select One";
    subPopOption.value = "-1";
    subPopSelect.appendChild(subPopOption);
    const subPopContainers = document.getElementsByClassName("sub_population_container");
    for (let i = 0; i < subPopContainers.length; i++) {
        const subPopContainer = subPopContainers.item(i);
        let subPopOption = document.createElement("option");
        let [subPopId, subPopName] = extractSubPopInfoFromContainer(subPopContainer);
        subPopOption.value = subPopId;
        if (subPopId.length === 0) {
            subPopId = "New";
        }
        subPopName += ` (${subPopId})`;
        subPopOption.text = subPopName;
        subPopSelect.appendChild(subPopOption);
    }

    return subPopSelect;
}

/**
 * Updates the sub-population select element class based on its value. This allows
 * us to modify its style to visually represent its status to the user.
 *
 * @param subPopSelectElement - The select element that is to have its UI
 * attributes updated
 */
function updateSubPopSelect(subPopSelectElement) {
    let selectClassName = "form-control"
    if (subPopSelectElement.value === "-1") {
        selectClassName += " e9y-red";
    }
    subPopSelectElement.className = selectClassName;
}

/**
 * Determines which is the currently selected sub-population based on the value and/or
 * text. For existing sub-populations, the value will be an integer that represents
 * the ID of the selected sub-population. For new sub-populations, the text will be
 * the sub-population name put into a specific format for consistency with other
 * sub-populations.
 *
 * @param subPopSelectElement - The sub-population select element of a criteria mapping
 * @param prevSubPopValue - The previous sub-population value that was in the select
 * element which is used to determine what the current selection should be.
 * @returns {number}
 */
function determineCurrentSelectedIndex(subPopSelectElement, prevSubPopValue) {
    let subPopOptions = subPopSelectElement.options;
    // If it's a number try using it as an ID of an existing sub-pop
    if (!isNaN(prevSubPopValue)) {
        for (let i = 0; i < subPopOptions.length; i++) {
            if (subPopOptions.item(i).value === `${prevSubPopValue}`) {
                return i;
            }
        }
    }

    // Check for a name match in case it is a newly created sub-pop that
    // hasn't been saved yet
    for (let i = 0; i < subPopOptions.length; i++) {
        if ((`<${subPopOptions.item(i).text}>`) === prevSubPopValue) {
            return i;
        }
    }

    // No matches found, return 0, which is the index of the "Select One" option
    return 0;
}

/**
 * Creates a row for the mapping table by cloning the list of cells for attribute keys,
 * adding the sub-population option elements, and selecting the one that matches the
 * sub-population mapping.
 *
 * @param attrCellList - A list of the keys encountered so far, stored in table cells
 * @param subPopOptions - The master copy of the sub-population option elements
 * @param subPopMapping - An indicator of which sub-population should be selected. For
 * existing sub-populations, the ID is used. For new sub-populations, the name is used.
 * @returns {HTMLTableRowElement}
 */
function createMappingTableRow(attrCellList, subPopOptions, subPopMapping) {
    // Create row and append to mappingTableBody
    let attrRow = document.createElement("tr");
    attrCellList.forEach(
        (attrCell) => {
            attrRow.appendChild(attrCell.cloneNode(true));
        }
    )
    // Add sub-population selection dropdown
    let subPopSelectionCell = document.createElement("td");
    let subPopOptionsClone = subPopOptions.cloneNode(true);
    subPopOptionsClone.criteriaKeys = attrCellList.map((keyCell) => keyCell.innerText);
    subPopOptionsClone.selectedIndex = determineCurrentSelectedIndex(
        subPopOptionsClone,
        subPopMapping
    );
    if (subPopOptionsClone.value === "-1") {
        subPopOptionsClone.className += " e9y-red";
    }
    subPopOptionsClone.disabled = globalWasActivated;
    subPopOptionsClone.onchange = handleSubPopSelectOnChange;
    subPopSelectionCell.appendChild(subPopOptionsClone);
    attrRow.appendChild(subPopSelectionCell);
    return attrRow;
}

/**
 * Recursively extracts the criteria map information into a table body (<tbody>). The function
 * clones all the table cells (<td>) created so far to create a new row for each mapping. It
 * will step into each map, adding a cell for each key along the way, until it finds the
 * mapping value. Once it finds the value, the function creates the row (<tr>) to contain
 * the cells of criteria keys and a select element of the sub-populations. The select element
 * is populated with clones of the passed in sub-population options, with the selected
 * option set by the mapping value. This row is then added to the table body.
 *
 * @param criteriaMap - The criteria map to be examined
 * @param attrCellList - A list of the keys encountered so far, stored in table cells
 * @param subPopOptions - The master copy of the sub-population option elements
 * @param mappingTableBody - The table body element to which the rows are to be added
 * @param currentRecursionDepth - The current depth of recursion, used to guard against infinite recursion
 *
 * @example
 *  Input:
 *      criteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 101,
 *                     "omega": 199
 *                 },
 *                 "2": {
 *                     "alpha": 201,
 *                     "omega": 299
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": 1,
 *                    "omega": 99
 *                 }
 *             }
 *         }
 *      attrCellList = ["a", "0", "omega"]
 *  Output:
 *      The function should update the select element to select the value matching the criteria
 *      map. In this case, it should be the sub-population with an ID of
 */
function extractMappingForTable(criteriaMap, attrCellList, subPopOptions, mappingTableBody, currentRecursionDepth = 0) {
    if (currentRecursionDepth > MAX_RECURSION_DEPTH) {
        console.error("Max recursion depth reached");
        return;
    }

    criteriaMap.forEach(
        (attrMapping, attrName) => {
            let attrCell = document.createElement("td");
            attrCell.innerText = attrName;
            attrCellList.push(attrCell);
            if (attrMapping instanceof Map) {
                extractMappingForTable(attrMapping, attrCellList, subPopOptions, mappingTableBody, currentRecursionDepth + 1);
            } else {
                // Create row and append to mappingTableBody
                mappingTableBody.appendChild(
                    createMappingTableRow(attrCellList, subPopOptions, attrMapping)
                );
            }
            attrCellList.pop();
        }
    )
}

/**
 * Creates the table body of the mapping table.
 *
 * @returns {HTMLTableSectionElement}
 */
function createMappingBody() {
    const mappingBody = document.createElement("tbody");
    const subPopOptions = createMappingSubPopOptions();
    extractMappingForTable(globalCriteriaMap, [], subPopOptions, mappingBody);
    return mappingBody;
}

/**
 * Updates the mapping table UI with the latest data based on the globalAttributeCriteriaList
 */
function updateMappingTable() {
    let mappingTableElement = document.getElementById("mapping_table");
    if (mappingTableElement !== null) {
        // Clear the table
        mappingTableElement.innerHTML = "";
        if ((globalAttributeCriteriaList === null) || (globalAttributeCriteriaList.length === 0)) {
            mappingTableElement.innerHTML = "<tbody><tr><td>There are no items in the table.</td></tr></tbody>";
        } else {
            const mappingHead = createMappingHead();
            mappingTableElement.appendChild(mappingHead);
            const mappingBody = createMappingBody();
            mappingTableElement.appendChild(mappingBody);
        }
    }
}

//#endregion

//#region Data Manipulation

/**
 * Creates and adds a new attribute criteria based on the name and options from the UI.
 * It will pop up an alert if an attribute with the same name already exists.
 *
 * @param newAttributeNameElement - The element with the new attribute name
 */
function addAttribute(newAttributeNameElement) {
    const newAttributeName = newAttributeNameElement.value.trim();
    if (newAttributeName.length > 0) {
        for (let attributeIndex = 0; attributeIndex < globalAttributeCriteriaList.length; attributeIndex++) {
            if (globalAttributeCriteriaList[attributeIndex].name === newAttributeName) {
                alert(`There is already an attribute named ${newAttributeName}.`);
                return;
            }
        }
        const newAttribute = createAttributeCriteria(newAttributeName, false, true);
        globalAttributeCriteriaList.push(newAttribute);
        newAttributeNameElement.value = "";
        propagateAttributesChange(globalAttributeCriteriaList);
    }
}

/**
 * First, prompts for a confirmation before deleting the selected attribute if confirmed.
 *
 * @param selectedAttributeIndex - The index of the selected attribute
 */
function deleteAttribute(selectedAttributeIndex) {
    if (confirm("Are you sure you want to delete this attribute? All related attribute values and mappings will also be deleted.")) {
        globalAttributeCriteriaList.splice(selectedAttributeIndex, 1);
        propagateAttributesChange(globalAttributeCriteriaList);
    }
}

/**
 * Creates and adds a new attribute value based on the value from the UI. It will pop up
 * an alert if the attribute value already exists. If the attribute value matches one of
 * the special attribute cases, it will first confirm the user's intent and then apply
 * the change to the option if it is confirmed.
 *
 * @param attributeIndex - The index of the selected attribute
 * @param newAttributeValueElement - The element with the new attribute value
 */
function addAttributeValue(attributeIndex, newAttributeValueElement) {
    const attributeValue = newAttributeValueElement.value.trim();
    if (attributeValue.length > 0) {
        let theAttribute = globalAttributeCriteriaList[attributeIndex];
        if (theAttribute.values.has(attributeValue)) {
            alert(`There is already an attribute value named ${attributeValue}.`);
            return;
        }

        if ((attributeValue === SPECIAL_CASE_ATTRIBUTES.IS_NULL) || (attributeValue === SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE)) {
            if (confirm(`You are trying to add a special attribute value (${attributeValue}). This will enable the option for the attribute. Do you wish to continue?`)) {
                if (attributeValue === SPECIAL_CASE_ATTRIBUTES.IS_NULL) {
                    theAttribute.allowNull = true;
                } else if (attributeValue === SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE) {
                    theAttribute.allowDefaultCase = true;
                }
            } else {
                return;
            }
        } else {
            theAttribute.values.add(attributeValue);
        }
        propagateAttributeValuesChange();
        newAttributeValueElement.value = "";
    }
}

/**
 * First prompts for a confirmation before deleting the selected attribute value if confirmed.
 * If the attribute value is for one of the special attribute handlers, it will confirm the
 * user's intent before applying the change to the specified option.
 *
 * @param attributeIndex - The index of the selected attribute
 * @param attributeValue - The attribute value to be deleted
 */
function deleteAttributeValue(attributeIndex, attributeValue) {
    let theAttribute = globalAttributeCriteriaList[attributeIndex];
    if ((attributeValue === SPECIAL_CASE_ATTRIBUTES.IS_NULL) || (attributeValue === SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE)) {
        if (confirm(`You are trying to delete a special attribute value (${attributeValue}). This will disable the option for the attribute. Do you wish to continue?`)) {
            if (attributeValue === SPECIAL_CASE_ATTRIBUTES.IS_NULL) {
                theAttribute.allowNull = false;
            } else if (attributeValue === SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE) {
                theAttribute.allowDefaultCase = false;
            }
            propagateAttributeValuesChange();
        }
    } else {
        if (confirm("Are you sure you want to delete this attribute value?")) {
            theAttribute.values.delete(attributeValue);
            propagateAttributeValuesChange();
        }
    }
}

/**
 * Updates the selected attribute's "allow_null" option based on the status of the UI
 *
 * @param attributeIndex - The index of the selected attribute
 */
function updateAttributeAllowNull(attributeIndex) {
    if (attributeIndex > -1) {
        const attributeAllowNull = document.getElementById(`attribute_allow_null_${attributeIndex}`);
        globalAttributeCriteriaList[attributeIndex].allowNull = attributeAllowNull.checked;
        propagateAttributeValuesChange();
    }
}

/**
 * Updates the selected attribute's "allow_default" option based on the status of the UI
 *
 * @param attributeIndex - The index of the selected attribute
 */
function updateAttributeAllowDefault(attributeIndex) {
    if (attributeIndex > -1) {
        const attributeAllowDefault = document.getElementById(`attribute_allow_default_${attributeIndex}`);
        globalAttributeCriteriaList[attributeIndex].allowDefaultCase = attributeAllowDefault.checked;
        propagateAttributeValuesChange();
    }
}

/**
 * Updates the globalCriteriaMap by first creating a new criteria map with keys based on
 * the globalAttributeCriteriaList and then filling in the sub-population info using
 * the values from the current globalCriteriaMap. Once the values have been updated
 * in the new criteria map, it will be used to replace the globalCriteriaMap.
 */
function updateCriteriaMapping() {
    // Populate new criteria mapping using globalAttributeCriteriaList
    let newCriteriaMap = createCriteriaMapFromAttributeCriteriaList(globalAttributeCriteriaList);

    // Fill in sub-population information using data from old criteria mapping
    populateSubPopInformation(globalCriteriaMap, newCriteriaMap);

    // Replace old criteria mapping with new one
    globalCriteriaMap = newCriteriaMap;

    // Update the UI
    updateSubPopLookupMapJSON();
    updateMappingTable();
}

/**
 * Refreshes the entire globalCriteriaMap by going through all of the sub-population
 * select elements.
 * ,
 * @type {HTMLCollectionOf<Element>}
 */
function refreshCriteriaMappingValues() {
    const subPopSelectElements = document.getElementsByClassName("sub-pop-select");
    for (let i = 0; i < subPopSelectElements.length; i++) {
        updateCriteriaMappingValues(subPopSelectElements[i]);
    }
}

/**
 * Updates the globalCriteriaMap using information in the subPopulationSelectElement. It
 * uses the stored keys to determine which mapping to update and then sets its value to
 * information based on the selected sub-population. If it is an existing sub-population,
 * it will store the ID, otherwise it will store name information.
 *
 * @param subPopSelectElement
 */
function updateCriteriaMappingValues(subPopSelectElement) {
    // Update data & map json field
    let finalMapping = globalCriteriaMap;
    const criteriaKeys = subPopSelectElement.criteriaKeys;
    for (let i = 0; i < criteriaKeys.length - 1; i++) {
        let tempMapping = finalMapping.get(criteriaKeys[i]);
        if (tempMapping === undefined) {
            tempMapping = finalMapping.get(SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE);
            if (tempMapping === undefined) {
                return;
            }
        }
        finalMapping = tempMapping
    }
    let finalValue = null;
    if (subPopSelectElement.value !== "-1") {
        if (subPopSelectElement.value.length > 0) {
            finalValue = parseInt(subPopSelectElement.value)
        } else {
            finalValue = `<${subPopSelectElement.options[subPopSelectElement.selectedIndex].text}>`;
        }
    }
    finalMapping.set(criteriaKeys[criteriaKeys.length - 1], finalValue);
    updateSubPopLookupMapJSON();
}

/**
 * Fills in sub-population information using data from old criteria mapping. The values set in the
 * new criteria map will be based on what those same members would have previously had. This means
 * DEFAULT_CASE values will get used to fill in new attribute values. If there is no matching
 * sub-population for the given criteria, null will be used instead and the user will need to
 * manually select the criteria's sub-population.
 *
 * @param origCriteriaMap
 * @param newCriteriaMap
 *
 * @example
 *  Input:
 *      origCriteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 101,
 *                     "omega": 199
 *                 },
 *                 "2": {
 *                     "alpha": 201,
 *                     "omega": 299
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": 1,
 *                    "omega": 99
 *                 }
 *             }
 *         }
 *      newCriteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 101,
 *                     "delta": null,
 *                     "omega": 199
 *                 },
 *                 "2": {
 *                     "alpha": 201,
 *                     "delta": null,
 *                     "omega": 299
 *                 },
 *                 "3": {
 *                     "alpha": null,
 *                     "delta": null,
 *                     "omega": null
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": 1,
 *                    "delta": null,
 *                    "omega": 99
 *                 }
 *             }
 *         }
 *  Updated:
 *      newCriteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 101,
 *                     "delta": null,
 *                     "omega": 199
 *                 },
 *                 "2": {
 *                     "alpha": 201,
 *                     "delta": null,
 *                     "omega": 299
 *                 },
 *                 "3": {
 *                     "alpha": 1,
 *                     "delta": null,
 *                     "omega": 99
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": 1,
 *                    "delta": null,
 *                    "omega": 99
 *                 }
 *             }
 *         }
 *      newCriteriaMap should be updated
 */
function populateSubPopInformation(origCriteriaMap, newCriteriaMap) {
    newCriteriaMap.forEach(
        (mapping, name) => {
            let origMapping = origCriteriaMap.get(name);
            if (origMapping === undefined) {
                origMapping = origCriteriaMap.get(SPECIAL_CASE_ATTRIBUTES.DEFAULT_CASE);
            }
            if (origMapping !== undefined) {
                if (mapping instanceof Map) {
                    if (origMapping instanceof Map) {
                        populateSubPopInformation(origMapping, mapping);
                    } else {
                        // Populate remaining entries with this value
                        fillInAllSubPopInformation(mapping, origMapping);
                    }
                } else {
                    if (origMapping instanceof Map) {
                        // Check if all remaining entries are a single value?
                        let subPopInfo = new Set();
                        getAllSubPopInformation(origMapping, subPopInfo);
                        if (subPopInfo.size === 1) {
                            newCriteriaMap.set(name, subPopInfo.values().next().value);
                        }
                    } else {
                        newCriteriaMap.set(name, origMapping);
                    }
                }
            }
        }
    );
}

/**
 * Recursively fills in all the sub-population information for the criteria
 * map. The main reason to do this is to fill in the sub-population info
 * when creating new branches using the information previously stored in
 * the parent.
 *
 * @param criteriaMap - The criteria map to fill in
 * @param subPopInfo - The information to be put into the criteria map
 * @param currentRecursionDepth - The current depth of recursion, used to guard against infinite recursion
 *
 * @example
 *  Input:
 *      criteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 101,
 *                     "omega": 199
 *                 },
 *                 "2": {
 *                     "alpha": 201,
 *                     "omega": 299
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": 1,
 *                    "omega": 99
 *                 }
 *             }
 *         }
 *      subPopInfo = 999
 *  Updated:
 *      criteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 999,
 *                     "omega": 999
 *                 },
 *                 "2": {
 *                     "alpha": 999,
 *                     "omega": 999
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": 999,
 *                    "omega": 999
 *                 }
 *             }
 *         }
 */
function fillInAllSubPopInformation(criteriaMap, subPopInfo, currentRecursionDepth = 0) {
    if (currentRecursionDepth > MAX_RECURSION_DEPTH) {
        console.error("Max recursion depth reached");
        return;
    }

    criteriaMap.forEach(
        (mapping, name) => {
            if (mapping instanceof Map) {
                fillInAllSubPopInformation(mapping, subPopInfo, currentRecursionDepth + 1);
            } else {
                criteriaMap.set(name, subPopInfo);
            }
        }
    )
}

/**
 * Recursively gathers all the sub-population information for the criteria
 * map. The main reason to do this is to determine if all "sub-trees" have
 * the same value when pruning. If all the sub-branches had the same info,
 * then the new leaf can use that info.
 *
 * @param criteriaMap - The criteria map to examine
 * @param existingSubPopInfo - The set of sub-population information that
 * has been observed so far, as well as where new sub-population should
 * be stored.
 * @param currentRecursionDepth - The current depth of recursion, used to guard against infinite recursion
 *
 * @example
 *  Input:
 *      criteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 101,
 *                     "omega": 199
 *                 }
 *             }
 *         }
 *      existingSubPopInfo = Set()
 *  Updated:
 *      existingSubPopInfo = Set({101, 199}
 */
function getAllSubPopInformation(criteriaMap, existingSubPopInfo, currentRecursionDepth = 0) {
    if (currentRecursionDepth > MAX_RECURSION_DEPTH) {
        console.error("Max recursion depth reached");
        return;
    }

    criteriaMap.forEach(
        (mapping, name) => {
            if (mapping instanceof Map) {
                getAllSubPopInformation(mapping, existingSubPopInfo, currentRecursionDepth + 1);
            } else {
                existingSubPopInfo.add(mapping);
            }
        }
    )
}

/**
 * Recursively replaces the mapped value in the criteria map. This is used
 * when the name information for new sub-populations have been changes. It
 * is necessary because, as they are new sub-populations and haven't been
 * written to the database yet, they do not have persistent IDs.
 *
 * @param criteriaMap - The criteria map to update
 * @param oldValue - The value to be replaced
 * @param newValue - The new value
 * @param currentRecursionDepth - The current depth of recursion, used to guard against infinite recursion
 *
 * @example
 *  Input:
 *      criteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 101,
 *                     "omega": 199
 *                 },
 *                 "2": {
 *                     "alpha": 201,
 *                     "omega": 299
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": 1,
 *                    "omega": "<Final opportunity (New)>"
 *                 }
 *             }
 *         }
 *      oldValue = "<Final opportunity (New)>"
 *      newValue = "<Last chance (New)>"
 *  Updated:
 *      criteriaMap = Nested Map equivalent of:
 *         {
 *             "a": {
 *                 "1": {
 *                     "alpha": 101,
 *                     "omega": 199
 *                 },
 *                 "2": {
 *                     "alpha": 201,
 *                     "omega": 299
 *                 },
 *                 "ATTRIBUTE_DEFAULT_CASE": {
 *                    "alpha": 1,
 *                    "omega": "<Last chance (New)>"
 *                 }
 *             }
 *         }
 */
function replaceCriteriaMappingValues(criteriaMap, oldValue, newValue, currentRecursionDepth = 0) {
    if (currentRecursionDepth > MAX_RECURSION_DEPTH) {
        console.error("Max recursion depth reached");
        return;
    }

    criteriaMap.forEach(
        (mapping, name) => {
            if (mapping instanceof Map) {
                replaceCriteriaMappingValues(mapping, oldValue, newValue, currentRecursionDepth + 1);
            } else {
                if (`${mapping}` === oldValue) {
                    criteriaMap.set(name, newValue);
                }
            }
        }
    )
}

//#endregion
