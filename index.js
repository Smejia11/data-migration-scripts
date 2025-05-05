import pLimit from "p-limit";
import { MongoClient } from "mongodb";


const limit = pLimit(50000);

const url = process.env.MONGODB_URI
const dbName = 'QuotationQAP';

function buildLeadView(lead, quotations = [], policies = []) {
    const dataLeadView = {
        ...lead,
    };
    const policyRequest = policies[0];
    const proposalData = policyRequest?.proposalData || {};
    const quotation = quotations[0];
    const quoteData = quotation?.quotationScreenValues?.quoteData || {};
    dataLeadView.quoteData = {}
    dataLeadView.proposalData = {}
    if (quotations.length > 0) {

        if (quoteData) {
            dataLeadView.quoteData.agentName = quoteData.agentName || proposalData?.agentName || null;
            dataLeadView.quoteData.nameInsure = quoteData.nameInsure || null;
            dataLeadView.quoteData.lastNameInsure = quoteData.lastNameInsure || null;
            dataLeadView.quoteData.userInformation = quoteData.userInformation || null;
            dataLeadView.quoteData.userBranchs = quoteData.userBranchs || null;

        }
    }

    if (policies.length > 0) {

        if (proposalData) {
            dataLeadView.proposalData.startDate = proposalData.startDate || null;
            dataLeadView.proposalData.endDate = proposalData.endDate || null;
            dataLeadView.proposalData.branch = proposalData.branch || null;
            dataLeadView.proposalData.branchName = proposalData.branchName || null;
        }
    }

    dataLeadView.placa = quoteData.placa || proposalData?.placa || null;

    return dataLeadView;
}

async function conectarMongo() {
    const client = new MongoClient(url, { useUnifiedTopology: true });

    try {
        await client.connect();
        console.log('Conectado a MongoDB');

        const db = client.db(dbName);
        const leadsCollection = db.collection('leads');
        const quotationsCollection = db.collection('quotations');
        const policyRequestsCollection = db.collection('policyrequests');

        // Consultamos todos los leads
        console.log('Consultando todos los leads...');
        console.time('Consulta leads');
        const leads = await leadsCollection.find({}).toArray();
        console.timeEnd('Consulta leads');
        console.log(`fin de consulta leads`);

        console.log(`Iniciando update de leads...`);
        console.time('Update leads');
        const tasks = leads.map((lead) =>
            limit(async () => {
                // Consultas a la base de datos para obtener las relaciones de `quotation` y `policy`
                const [quotations, policies] = await Promise.all([
                    quotationsCollection.find({ _id: lead.quotation }).toArray(),
                    policyRequestsCollection.find({ quotation: lead.quotation }).toArray(),
                ]);

                // Construcción del objeto `leadView`
                const leadView = buildLeadView(lead, quotations, policies);

                // Actualización de la base de datos con el objeto `leadView`
                await leadsCollection.updateOne(
                    { _id: lead._id },
                    { $set: leadView }
                );
            })

        );
        await Promise.all(tasks);
        console.timeEnd('Update leads');
        console.log('Todos los leads han sido procesados y actualizados.');
    } catch (error) {
        console.error('Error de conexión a MongoDB:', error);
    }
}

conectarMongo();