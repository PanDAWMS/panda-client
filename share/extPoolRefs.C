#include <TROOT.h>
#include <TChain.h>
#include <TFile.h>

void extPoolRefs()
{
  std::string arg;

  TChain *m_tree = new TChain("CollectionTree");
  while (true)
    {
      std::cin >> arg;
      // delimiter
      if (arg == "---")
	break;
      // add 
      m_tree->Add(arg.c_str());
      std::cout << "Adding " << arg << std::endl;
    } 

  // set address
  Char_t Token[153];
  Char_t StreamAOD_ref[153];
  Char_t StreamESD_ref[153];
  Char_t StreamRDO_ref[153];
  Char_t StreamRAW_ref[153];
  Char_t Stream1_ref[153];
  m_tree->SetBranchAddress("Token",Token);
  m_tree->SetBranchAddress("StreamAOD_ref",StreamAOD_ref);
  m_tree->SetBranchAddress("StreamESD_ref",StreamESD_ref);
  m_tree->SetBranchAddress("StreamRDO_ref",StreamRDO_ref);
  m_tree->SetBranchAddress("StreamRAW_ref",StreamRAW_ref);
  m_tree->SetBranchAddress("Stream1_ref",  Stream1_ref);

  // loop over all entries
  std::string previousOne;
  std::string previousAOD;
  std::string previousESD;
  std::string previousRDO;
  std::string previousRAW;
  std::string previous1;
  Long64_t nentries = m_tree->GetEntries();
  std::cout << "=============" << std::endl;
  for (Long64_t jentry=0; jentry<nentries;++jentry)
    {
      // get entry
      m_tree->GetEntry(jentry);
      // extract DB for AOD
      std::string currentOne = Token;
      currentOne = currentOne.substr(0,currentOne.find(']'));
      // print if new one
      if (currentOne != previousOne)
	{
	  std::cout << "PoolRef: " << currentOne << "]" << std::endl;
	  previousOne = currentOne;
	}
      // extract DB for AOD
      std::string currentAOD = StreamAOD_ref;
      currentAOD = currentAOD.substr(0,currentAOD.find(']'));
      // print if new one
      if (currentAOD != previousAOD)
	{
	  std::cout << "PoolRef: " << currentAOD << "]" << std::endl;
	  previousAOD = currentAOD;
	}
      // extract DB for ESD
      std::string currentESD = StreamESD_ref;
      currentESD = currentESD.substr(0,currentESD.find(']'));
      // print if new one
      if (currentESD != previousESD)
	{
	  std::cout << "ESD Ref: " << currentESD << "]" << std::endl;
	  previousESD = currentESD;
	}
      // extract DB for RDO
      std::string currentRDO = StreamRDO_ref;
      currentRDO = currentRDO.substr(0,currentRDO.find(']'));
      // print if new one
      if (currentRDO != previousRDO)
	{
	  std::cout << "RDO Ref: " << currentRDO << "]" << std::endl;
	  previousRDO = currentRDO;
	}
      // extract DB for RAW
      std::string currentRAW = StreamRAW_ref;
      currentRAW = currentRAW.substr(0,currentRAW.find(']'));
      // print if new one
      if (currentRAW != previousRAW)
	{
	  std::cout << "RAW Ref: " << currentRAW << "]" << std::endl;
	  previousRAW = currentRAW;
	}
      // extract DB for Stream1
      std::string current1 = Stream1_ref;
      current1 = current1.substr(0,current1.find(']'));
      // print if new one
      if (current1 != previous1)
	{
	  std::cout << "St1 Ref: " << current1 << "]" << std::endl;
	  previous1 = current1;
	}
    }
  std::cout << "=============" << std::endl;
}
