import pLimit from "p-limit";
import { MongoClient } from "mongodb";

const limiter = Number(process.env.LIMITER) || 1000;
const limit = pLimit(limiter);

const url = process.env.MONGODB_URI
const dbName = process.env.MONGODB_DB_NAME;

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
            dataLeadView.quoteData.userBranchs = quoteData.userBranchs || null

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

async function createIndex(db) {
    try {
        db.collection('leads').createIndex({ agentCode: 1 });
        db.collection('leads').createIndex({ agentCode: 1, "quoteData.userInformation.userEmail": 1 });
        db.collection('leads').createIndex({ createdAt: -1 });
        db.collection('leads').createIndex({ date: -1 });
        db.collection('leads').createIndex({ date: -1, placa: 1 });
        db.collection('leads').createIndex({ ipAddress: 1 });
        db.collection('leads').createIndex({ placa: 1 });
        db.collection('leads').createIndex({ quotationNumber: 1 });
        db.collection('leads').createIndex({ quotation: 1 });
        db.collection('leads').createIndex({ "quoteData.userBranchs.branchId": 1, date: -1 });
        db.collection('leads').createIndex({ "quoteData.userBranchs.branchId": 1, date: -1, placa: 1 });
        db.collection('leads').createIndex({ riskType: 1, status: 1 });
        db.collection('leads').createIndex({ status: 1, date: -1 });
        db.collection('leads').createIndex({ updatedAt: -1 });
        db.collection("quotations").createIndex({ idInternal: 1 })
    } catch (error) {
        throw new Error(`Error creating indexes: ${error.message}`);
    }
}

async function conectarMongo() {
    const client = new MongoClient(url, { monitorCommands: true });

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
        console.log('Iniciando creación de índices...');
        console.time('Creación de índices');
        await createIndex(db);
        console.timeEnd('Creación de índices');
    } catch (error) {
        console.error('Error de conexión a MongoDB:', error);
    }
}



conectarMongo();