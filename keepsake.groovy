import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

class ModifyShopifyOrderJson implements StreamCallback {
    private static final String FACILITY_MAPPING_FILE_PATH = "gorjana/uat/shipsiFacilityLocation/Facility_Identification_For_Shipsi.json"

    @Override
    void process(InputStream inputStream, OutputStream outputStream) throws IOException {
        // Read the input FlowFile content
        def ordersList = readContentFromFlowFile(inputStream)

        // List of SKUs to check
        def skusToCheck = [
                "228-104-185-G", "2210-106-185-G", "228-101-185-G", "244-104-185-G",
                "244-105-185-G", "244-106-185-G", "223-100-G", "2212-200-185-G",
                "2212-201-185-G", "2212-202-185-G", "2212-200-185-WG", "2212-201-185-WG",
                "2212-202-185-WG", "227-206-185-G", "227-205-185-G", "227-204-185-G",
                "227-206-185-WG", "227-205-185-WG", "227-204-185-WG", "228-202-185-G",
                "2310-202-185-G", "228-011-185-G", "228-012-185-G", "2310-3005-185-G",
                "2310-3006-185-G", "2310-3007-185-G", "2310-3008-185-G", "2310-3009-185-G"
        ]

        // Read facility mapping from file
        def facilityMapping = readFacilityMapping()

        // Iterate over each order in the list and apply modifications
        ordersList.each { order ->
            checkAndAddShipBookletTag(order)
            checkAndAddSku(order, skusToCheck)
            checkAndAddGiftWrapTogether(order)
            checkAndAddShippingFacility(order, facilityMapping)
            checkAndAddGiftWrapNote(order)
        }

        // Write the modified JSON to the output FlowFile
        writeContentToFlowFile(outputStream, ordersList)
    }

    // Method to read content from the FlowFile
    private def readContentFromFlowFile(InputStream inputStream) {
        def text = inputStream.getText(StandardCharsets.UTF_8.name())
        def jsonSlurper = new JsonSlurper()
        return jsonSlurper.parseText(text)
    }

    // Method to read facility mapping from file
    private def readFacilityMapping() {
        def jsonSlurper = new JsonSlurper()
        def fileContent = new String(Files.readAllBytes(Paths.get(FACILITY_MAPPING_FILE_PATH)), StandardCharsets.UTF_8)
        return jsonSlurper.parseText(fileContent)
    }

    // Method to write content to the FlowFile
    private void writeContentToFlowFile(OutputStream outputStream, def content) {
        def outputJson = JsonOutput.toJson(content)
        outputStream.write(JsonOutput.prettyPrint(outputJson).getBytes(StandardCharsets.UTF_8))
    }

    // Method to check and add 'isShipBooklet' attribute
    private void checkAndAddShipBookletTag(def order) {
        if (order.tags.contains("shipBooklet")) {
            def newNoteAttribute = [
                    name: "isShipBooklet",
                    value: "true"
            ]
            order.note_attributes << newNoteAttribute
        }
    }

    // Method to check for specific SKUs and add a new line item with updated fields
    private void checkAndAddSku(def order, List<String> skusToCheck) {
        def newLineItems = []

        order.line_items.each { lineItem ->
            if (skusToCheck.contains(lineItem.sku)) {
                // Add the 'id' property to the original line item as well
                lineItem.properties << [name: "id", value: lineItem.id]


                def newLineItem = [
                        attributed_staffs: [],
                        fulfillable_quantity: lineItem.fulfillable_quantity,
                        fulfillment_service: lineItem.fulfillment_service,
                        fulfillment_status: lineItem.fulfillment_status,
                        gift_card: false,
                        grams: 0,
                        name: "Keepsake Box",
                        price: "0.00",
                        product_exists: true,
                        product_id: 7238569230380,
                        properties: lineItem.properties,
                        quantity: lineItem.current_quantity,
                        requires_shipping: true,
                        sku: "PKG245",
                        taxable: true,
                        title: "Keepsake Box",
                        variant_id: 42132429471788,
                        variant_inventory_management: lineItem.variant_inventory_management,
                        variant_title: null,
                        vendor: lineItem.vendor
                ]
                newLineItems << newLineItem
            }
        }

        order.line_items.addAll(newLineItems)
    }

    // Method to check and add line item property if gift wrap option is 'Gift Wrap Items Together'
    private void checkAndAddGiftWrapTogether(def order) {
        def giftWrapTogether = order.note_attributes.find {
            it.name == 'Gift Wrap Option' && it.value == 'Gift Wrap Items Together'
        }
        if (giftWrapTogether) {
            // Find the first line item with the property 'gift wrap'
            def firstGiftWrapLineItem = order.line_items.find { lineItem ->
                lineItem.properties.any { it.name == 'Gift Wrap' }
            }

            if (firstGiftWrapLineItem) {
                def firstLineItemId = firstGiftWrapLineItem.id
                order.line_items.each { lineItem ->
                    if (lineItem.properties.any { it.name == 'Gift Wrap' }) {
                        // Overwrite or add the 'id' property
                        def idProperty = [name: "id", value: firstLineItemId]
                        lineItem.properties.removeIf { it.name == "id" }
                        lineItem.properties << idProperty
                    }
                }
            }
        }
    }

    // Method to check and add '_hcShippingFacility' property if 'HC_PRE_SELECTED_FAC' tag is present
    private void checkAndAddShippingFacility(def order, def facilityMapping) {
        if (order.tags.contains("HC_PRE_SELECTED_FAC")) {
            def shippingCode = order.shipping_lines[0]?.code?.split('-')?.first()
            if (shippingCode) {
                def facility = facilityMapping.find { it.id_value == shippingCode }
                if (facility) {
                    order.line_items.each { lineItem ->
                        def hcShippingFacility = [name: "_hcShippingFacility", value: facility.facility_id]
                        lineItem.properties << hcShippingFacility
                    }
                }
            }
            order.shipping_lines[0].title = 'Shipsi'
        }
    }

    // Method to check and add 'note' field if 'Gift Wrap Note' is present in note_attributes
    private void checkAndAddGiftWrapNote(def order) {
        def giftWrapNote = order.note_attributes.find { it.name == 'Gift Wrap Note' }
        if (giftWrapNote) {
            order.note = giftWrapNote.value
        }
    }
}

flowFile = session.get()
if (flowFile != null) {
    try {
        flowFile = session.write(flowFile, new ModifyShopifyOrderJson())
        session.transfer(flowFile, REL_SUCCESS)
    } catch (Exception e) {
        flowFile = session.putAttribute(flowFile, "groovy.error", e.toString())
        session.transfer(flowFile, REL_FAILURE)
    }
}
